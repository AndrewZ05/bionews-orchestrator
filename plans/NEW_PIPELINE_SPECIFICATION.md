# New Pipeline Specification

## Overview

This document provides the complete specification for adding new data extraction pipelines to the orchestrator system. Every pipeline MUST follow these patterns exactly. NO deviations are allowed without explicit approval.

## Critical Rules

### Code Style Requirements

1. **NO EMOJIS**: Never use emojis anywhere in code, comments, log messages, or output
2. **Single-line comments only**: Use `# comment` format, NEVER use `"""comment"""` for comments
3. **Docstrings**: Multi-line docstrings are allowed ONLY for function/class documentation using triple quotes
4. **Backward compatibility**: New code MUST NOT break existing functionality
5. **No modifications without approval**: Do not modify core shared utilities or patterns without consultation

### Mandatory Pattern Compliance

Every new pipeline MUST:
- Use the standard metadata helper function `set_execution_metadata()`
- Define schemas in YAML configuration files (NEVER JSON)
- Thread `execution_id` through the entire call chain
- Follow the exact same structure as existing extractors (Facebook, Mailchimp, WordPress, LimeSurvey, DCM)

---

## Pipeline Architecture

### Data Flow

```
Source API/Database
    ↓
Extractor Plugin (plugins/{source}_extractor.py)
    ↓
DataFrame/Records with Metadata
    ↓
Parquet Files (local)
    ↓
GCS Upload (gcs://{bucket}/{source}/raw/)
    ↓
BigQuery External Table (staging dataset)
    ↓
Hash Merge to Production Table (production dataset)
```

### Required Components

Every pipeline requires:

1. **YAML Configuration File**: `configs/{source}.yaml`
2. **Extractor Plugin**: `plugins/{source}_extractor.py`
3. **Environment Variables**: `.env` entries for credentials
4. **BigQuery Datasets**: Staging and production datasets

---

## Step 1: YAML Configuration File

### File Location
`configs/{source}.yaml`

### Required Sections

#### 1.1 Source Configuration

```yaml
source:
  name: {source_name}
  type: {api|mysql|postgres|rest}
  plugin: {source}_extractor

  connection:
    # Connection details specific to source type
    # API: api_key, base_url, etc.
    # Database: host, port, database, user, password
```

#### 1.2 Pipeline Configuration

```yaml
pipeline:
  name: {source}_pipeline
  staging_dataset: {source}_staging
  production_dataset: {source}_data
  test_dataset: {source}_test
  gcs_bucket: ${GCS_BUCKET}
  gcs_path: {source}/raw
  gcs_staging_path: {source}/staging
  parallel_workers: 5  # Adjust based on API rate limits
  batch_size: 1000
  project: ${GCP_PROJECT_ID:-bi-data-391216}
```

#### 1.3 Table Groups

```yaml
groups:
  core:
    description: Core tables required for basic functionality
    tables:
      - table1
      - table2

  all:
    description: All available tables
    tables:
      - table1
      - table2
      - table3
```

#### 1.4 Resources (Tables)

**CRITICAL**: Every table MUST have a `schema` section defined in YAML.

```yaml
resources:
  table_name:
    active: true
    description: Description of what this table contains
    table_name: {source}_{table_name}
    group: core
    tenant_field: {field_name}  # Field used for multi-tenant partitioning

    primary_key:
      - field1
      - field2

    # Incremental loading strategy (optional)
    incremental_strategy: date  # Options: date, id, none
    incremental_date_fields:
      - updated_at
      - created_at
    incremental_lookback_days: 7

    # BigQuery partitioning (optional)
    partitioning:
      field: date
      type: DAY

    # BigQuery clustering (optional)
    clustering:
      - tenant_field
      - date

    # REQUIRED: Schema definition
    schema:
      # Business fields
      id: INTEGER
      name: STRING
      created_at: TIMESTAMP
      updated_at: TIMESTAMP

      # REQUIRED: Metadata fields (these MUST be included in every table)
      extracted_at: TIMESTAMP
      execution_id: STRING
      source: STRING

      # Source-specific metadata fields (if applicable)
      {source}_account_id: STRING
      {source}_account_name: STRING

    # Schema metadata (auto-updated by system)
    schema_metadata:
      last_updated: '2025-11-21T00:00:00.000000'
      discovery_source: manual_definition
      column_count: 8
```

#### 1.5 Destination Configuration

```yaml
destination:
  bigquery:
    project_id: ${GCP_PROJECT_ID}
    dataset: {source}_data
    write_disposition: WRITE_APPEND
    create_disposition: CREATE_IF_NEEDED
    table_prefix: ""
    table_suffix: ""
```

#### 1.6 Processing Configuration

```yaml
processing:
  local_dir: data/{source}
  max_workers: 5

  error_handling:
    retry_attempts: 3
    retry_delay: 60
    continue_on_error: true
```

#### 1.7 Monitoring Configuration

```yaml
monitoring:
  enabled: true

  email_notifications:
    enabled: false
    recipients: []

  slack_notifications:
    enabled: false
    webhook_url: ${SLACK_WEBHOOK_URL}
```

---

## Step 2: Extractor Plugin

### File Location
`plugins/{source}_extractor.py`

### Required Imports

```python
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from shared.account_context import set_execution_metadata
from shared.config_loader import load_yaml_config
from shared.gcs_pipeline import run_gcs_pipeline

logger = logging.getLogger(__name__)
```

### Required Functions

#### 2.1 Main Entry Point

```python
def run_pipeline(
    source: str = "{source}",
    env: str = "prod",
    table: Optional[str] = None,
    group: Optional[str] = None,
    rebuild: bool = False,
    test_mode: bool = False,
    validate_only: bool = False,
    skip_post_process: bool = False,
    execution_id: Optional[str] = None,
) -> bool:
    """
    Main pipeline orchestration for {source} data extraction.

    Args:
        source: Source name (always '{source}')
        env: Environment (prod/staging/test)
        table: Specific table to extract
        group: Table group to extract
        rebuild: Rebuild from scratch
        test_mode: Test mode flag
        validate_only: Validate without loading to BigQuery
        skip_post_process: Skip post-processing steps
        execution_id: Unique execution identifier

    Returns:
        True if successful, False otherwise
    """
    try:
        # Generate execution ID if not provided
        if not execution_id:
            execution_id = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

        logger.info(f"Starting {source} pipeline - execution_id: {execution_id}")

        # Load configuration
        config = load_yaml_config(f"configs/{source}.yaml")

        # Determine which tables to process
        tables_to_process = _get_tables_to_process(config, table, group)

        # Process each table
        success = True
        for table_name in tables_to_process:
            table_config = config['resources'][table_name]

            # Extract data
            extracted = extract_resource(
                table_config=table_config,
                config=config,
                test_mode=test_mode,
                execution_id=execution_id
            )

            if not extracted:
                logger.error(f"Failed to extract {table_name}")
                success = False
                continue

            # Run GCS pipeline (upload to GCS, load to BigQuery)
            if not validate_only:
                pipeline_success = run_gcs_pipeline(
                    source=source,
                    table_name=table_config['table_name'],
                    config=config,
                    execution_id=execution_id,
                    skip_post_process=skip_post_process
                )

                if not pipeline_success:
                    logger.error(f"Pipeline failed for {table_name}")
                    success = False

        return success

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return False


def _get_tables_to_process(
    config: Dict[str, Any],
    table: Optional[str],
    group: Optional[str]
) -> List[str]:
    """
    Determine which tables to process based on arguments.

    Args:
        config: YAML configuration
        table: Specific table name
        group: Table group name

    Returns:
        List of table names to process
    """
    if table:
        return [table]

    if group:
        return config['groups'][group]['tables']

    # Default to 'core' group
    return config['groups']['core']['tables']
```

#### 2.2 Resource Extraction Function

```python
def extract_resource(
    table_config: Dict[str, Any],
    config: Dict[str, Any],
    test_mode: bool = False,
    execution_id: str = None
) -> bool:
    """
    Extract data for a specific resource/table.

    CRITICAL: This function MUST receive execution_id as a parameter.

    Args:
        table_config: Table configuration from YAML
        config: Full configuration object
        test_mode: Test mode flag
        execution_id: Execution identifier (REQUIRED)

    Returns:
        True if successful, False otherwise
    """
    try:
        table_name = table_config['table_name']
        logger.info(f"Extracting {table_name}")

        # Fetch data from source (API, database, etc.)
        records = _fetch_data_from_source(table_config, config)

        if not records:
            logger.warning(f"No data fetched for {table_name}")
            return True  # Not an error, just no data

        # CRITICAL: Apply standard metadata using helper function
        # This is MANDATORY for all extractors
        set_execution_metadata(records, execution_id, source='{source}')

        # Convert to DataFrame
        df = pd.DataFrame(records)

        # Add source-specific metadata (if needed)
        # Example: df['{source}_account_id'] = account_id

        # Save to Parquet
        local_dir = Path(config['processing']['local_dir'])
        local_dir.mkdir(parents=True, exist_ok=True)

        parquet_path = local_dir / f"{table_name}.parquet"
        df.to_parquet(parquet_path, index=False)

        logger.info(f"Extracted {len(df)} rows to {parquet_path}")

        return True

    except Exception as e:
        logger.error(f"Failed to extract resource: {e}", exc_info=True)
        return False
```

#### 2.3 Data Fetching Function

```python
def _fetch_data_from_source(
    table_config: Dict[str, Any],
    config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Fetch data from the source system (API, database, etc.).

    Args:
        table_config: Table configuration
        config: Full configuration

    Returns:
        List of records as dictionaries
    """
    # Implementation depends on source type:
    # - REST API: Use requests library
    # - Database: Use SQLAlchemy or native drivers
    # - File: Use pandas read functions

    records = []

    # Example for REST API:
    # client = _get_api_client(config)
    # response = client.get(table_config['endpoint'])
    # records = response.json()

    # Example for Database:
    # connection = _get_db_connection(config)
    # query = f"SELECT * FROM {table_config['table_name']}"
    # records = connection.execute(query).fetchall()

    return records
```

### Standard Metadata Application Pattern

**CRITICAL**: This is the ONLY acceptable pattern for adding metadata fields.

```python
# CORRECT PATTERN (DataFrame-based extractors)
records = df.to_dict('records')
set_execution_metadata(records, execution_id, source='{source}')
df = pd.DataFrame(records)

# CORRECT PATTERN (List-based extractors)
records = []  # Fetch from API/database
set_execution_metadata(records, execution_id, source='{source}')
# Continue processing with records

# INCORRECT PATTERN (DO NOT USE)
df['execution_id'] = execution_id  # WRONG - bypasses standard helper
df['source'] = '{source}'  # WRONG - bypasses standard helper
df['extracted_at'] = datetime.now()  # WRONG - bypasses standard helper
```

### Execution ID Threading

**CRITICAL**: The `execution_id` parameter MUST be threaded through the entire call chain.

```python
# run_pipeline() receives execution_id
def run_pipeline(..., execution_id: str = None):
    if not execution_id:
        execution_id = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

    # Pass to extract_resource
    extract_resource(..., execution_id=execution_id)

# extract_resource() receives execution_id
def extract_resource(..., execution_id: str = None):
    # Pass to any sub-functions that process data
    process_account(..., execution_id=execution_id)

    # Use in set_execution_metadata
    set_execution_metadata(records, execution_id, source='{source}')

# All sub-functions must accept execution_id
def process_account(..., execution_id: str = None):
    # Use execution_id when calling set_execution_metadata
    set_execution_metadata(records, execution_id, source='{source}')
```

---

## Step 3: Environment Variables

### File Location
`.env`

### Required Variables

```bash
# Source-specific credentials
{SOURCE}_API_KEY=your_api_key_here
{SOURCE}_API_SECRET=your_api_secret_here
{SOURCE}_BASE_URL=https://api.example.com

# Database credentials (if applicable)
{SOURCE}_DB_HOST=localhost
{SOURCE}_DB_PORT=3306
{SOURCE}_DB_NAME=database_name
{SOURCE}_DB_USER=username
{SOURCE}_DB_PASSWORD=password

# Google Cloud (shared)
GCP_PROJECT_ID=bi-data-391216
GCS_BUCKET=your-gcs-bucket
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

---

## Step 4: BigQuery Setup

### Create Datasets

```bash
# Staging dataset (for external tables)
bq mk --dataset \
    --location=US \
    --description="Staging dataset for {source} data" \
    bi-data-391216:{source}_staging

# Production dataset (for production tables)
bq mk --dataset \
    --location=US \
    --description="Production dataset for {source} data" \
    bi-data-391216:{source}_data

# Test dataset (optional, for testing)
bq mk --dataset \
    --location=US \
    --description="Test dataset for {source} data" \
    bi-data-391216:{source}_test
```

---

## Step 5: Testing Checklist

### Pre-Deployment Testing

- [ ] Syntax validation: `python -m py_compile plugins/{source}_extractor.py`
- [ ] YAML validation: Load config without errors
- [ ] Test mode execution: `python orchestrate.py --source {source} --env prod --group core --test`
- [ ] Validate-only mode: `python orchestrate.py --source {source} --env prod --group core --validate-only`
- [ ] Single table extraction: `python orchestrate.py --source {source} --env prod --table {table_name}`
- [ ] Full pipeline: `python orchestrate.py --source {source} --env prod --group core`
- [ ] Verify staging table has data: `SELECT COUNT(*) FROM {source}_staging.{table_name}`
- [ ] Verify production table has data: `SELECT COUNT(*) FROM {source}_data.{table_name}`
- [ ] Verify metadata fields exist: `SELECT execution_id, source, extracted_at FROM {source}_data.{table_name} LIMIT 1`

### Required Metadata Fields Verification

Run this query for EVERY table:

```sql
SELECT
    execution_id,
    source,
    extracted_at
FROM `bi-data-391216.{source}_data.{table_name}`
LIMIT 1
```

**Expected Results**:
- `execution_id`: Should be a timestamp string (e.g., '20251121_143000')
- `source`: Should be '{source}' (e.g., 'dcm', 'mailchimp', 'facebook')
- `extracted_at`: Should be a timestamp (e.g., '2025-11-21 14:30:00 UTC')

If ANY of these fields are missing or NULL, the pipeline is BROKEN and must be fixed.

---

## Step 6: Common Patterns

### Multi-Account/Tenant Pattern

For sources with multiple accounts/tenants:

```python
def extract_resource(..., execution_id: str = None):
    # Get list of accounts
    accounts = _get_accounts(config)

    all_records = []

    for account in accounts:
        # Fetch data for this account
        account_records = _fetch_account_data(account, table_config, config)

        # Add account-specific metadata
        for record in account_records:
            record['{source}_account_id'] = account['id']
            record['{source}_account_name'] = account['name']

        all_records.extend(account_records)

    # Apply standard metadata to ALL records
    set_execution_metadata(all_records, execution_id, source='{source}')

    # Convert to DataFrame and save
    df = pd.DataFrame(all_records)
    df.to_parquet(parquet_path, index=False)
```

### Incremental Loading Pattern

```python
def _get_date_range(table_config: Dict[str, Any]) -> Tuple[str, str]:
    """Get date range for incremental loading."""
    strategy = table_config.get('incremental_strategy')

    if strategy == 'date':
        lookback_days = table_config.get('incremental_lookback_days', 7)
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=lookback_days)
        return str(start_date), str(end_date)

    # Full load
    return table_config.get('start_date', '2020-01-01'), 'today'
```

### Error Handling Pattern

```python
def extract_resource(...):
    try:
        # Extraction logic
        records = _fetch_data_from_source(table_config, config)

        if not records:
            logger.warning(f"No data for {table_name}")
            return True  # Not an error

        # Process and save
        set_execution_metadata(records, execution_id, source='{source}')
        df = pd.DataFrame(records)
        df.to_parquet(parquet_path, index=False)

        return True

    except Exception as e:
        logger.error(f"Extraction failed: {e}", exc_info=True)
        return False
```

---

## Step 7: Integration with Orchestrator

### Command-Line Interface

The orchestrator MUST support these arguments:

```bash
# Extract specific table
python orchestrate.py --source {source} --env prod --table {table_name}

# Extract table group
python orchestrate.py --source {source} --env prod --group core

# Test mode (process only first account/record)
python orchestrate.py --source {source} --env prod --group core --test

# Validate only (don't load to BigQuery)
python orchestrate.py --source {source} --env prod --group core --validate-only

# Rebuild from scratch
python orchestrate.py --source {source} --env prod --group core --rebuild

# Skip post-processing
python orchestrate.py --source {source} --env prod --group core --skip-post-process

# Set lookback days (for incremental loads)
python orchestrate.py --source {source} --env prod --table {table_name} --lookback 30
```

### Orchestrate.py Integration

Add to `orchestrate.py`:

```python
elif args.source == '{source}':
    from plugins.{source}_extractor import run_pipeline
    success = run_pipeline(
        source=args.source,
        env=args.env,
        table=args.table,
        group=args.group,
        rebuild=args.rebuild,
        test_mode=args.test,
        validate_only=args.validate_only,
        skip_post_process=args.skip_post_process,
        execution_id=execution_id
    )
```

---

## Step 8: Schema Management

### Schema Definition Rules

1. **YAML is the single source of truth**: All schemas MUST be defined in YAML config files
2. **No JSON schemas**: Do not create `shared/schemas/{table}.json` files
3. **Include metadata fields**: Every schema MUST include `extracted_at`, `execution_id`, `source`
4. **Use standard BigQuery types**: STRING, INTEGER, FLOAT, TIMESTAMP, BOOLEAN, DATE
5. **Document field purpose**: Add descriptions where helpful

### Schema Update Pattern

```yaml
resources:
  table_name:
    schema:
      # Auto-updated by schema discovery
      id: INTEGER
      name: STRING

      # REQUIRED metadata fields
      extracted_at: TIMESTAMP
      execution_id: STRING
      source: STRING

    schema_metadata:
      last_updated: '2025-11-21T14:30:00.000000'
      discovery_source: runtime_auto_update
      column_count: 5
```

---

## Step 9: Documentation Requirements

### Required Documentation

1. **Config file comments**: Add inline comments explaining non-obvious settings
2. **Function docstrings**: Every function MUST have a docstring
3. **README update**: Update main README.md with new source
4. **Example queries**: Provide sample SQL queries in `sql/{source}/` directory

### Example README Entry

```markdown
## {Source Name} Pipeline

### Description
Brief description of what this pipeline extracts.

### Configuration
- **Config file**: `configs/{source}.yaml`
- **Plugin**: `plugins/{source}_extractor.py`
- **Tables**: See config file for full list

### Usage

# Extract core tables
python orchestrate.py --source {source} --env prod --group core

# Extract specific table
python orchestrate.py --source {source} --env prod --table {table_name}

### Tables

| Table Name | Description | Incremental |
|------------|-------------|-------------|
| {table1} | Description | Yes (date) |
| {table2} | Description | No |

### Dependencies
- Python packages: requests, pandas, etc.
- External services: API credentials required
```

---

## Step 10: Quality Assurance

### Code Review Checklist

Before considering a pipeline complete:

- [ ] No emojis in code, comments, or logs
- [ ] Single-line comments use `#` format
- [ ] `set_execution_metadata()` is used (not manual field assignment)
- [ ] `execution_id` is threaded through all functions
- [ ] Schema defined in YAML (not JSON)
- [ ] All required metadata fields present in schema
- [ ] Backward compatibility maintained
- [ ] Error handling implemented
- [ ] Logging statements use logger (not print)
- [ ] Test mode works correctly
- [ ] Validate-only mode works correctly
- [ ] Production table has expected row count
- [ ] Metadata fields populated correctly
- [ ] No hardcoded values (use config/env vars)
- [ ] Function signatures match standard pattern
- [ ] Documentation updated

### Performance Checklist

- [ ] Parallel processing configured appropriately
- [ ] Batch size optimized
- [ ] API rate limiting respected
- [ ] Memory usage acceptable (use chunking for large datasets)
- [ ] Incremental loading implemented where possible

---

## Appendix A: Standard Function Signatures

### run_pipeline()

```python
def run_pipeline(
    source: str = "{source}",
    env: str = "prod",
    table: Optional[str] = None,
    group: Optional[str] = None,
    rebuild: bool = False,
    test_mode: bool = False,
    validate_only: bool = False,
    skip_post_process: bool = False,
    execution_id: Optional[str] = None,
) -> bool:
```

### extract_resource()

```python
def extract_resource(
    table_config: Dict[str, Any],
    config: Dict[str, Any],
    test_mode: bool = False,
    execution_id: str = None
) -> bool:
```

### process_account() (for multi-account sources)

```python
def process_account(
    account_id: str,
    table_config: Dict[str, Any],
    config: Dict[str, Any],
    test_mode: bool = False,
    execution_id: str = None
) -> bool:
```

---

## Appendix B: Reference Implementations

Study these extractors as reference implementations:

1. **Facebook** (`plugins/facebook_extractor.py`): REST API with multi-account pattern
2. **Mailchimp** (`plugins/mailchimp_extractor.py`): REST API with complex nested data
3. **WordPress** (`plugins/wordpress_extractor.py`): REST API with multi-site pattern
4. **LimeSurvey** (`plugins/limesurvey_extractor.py`): MySQL database with SSH tunnel
5. **DCM** (`plugins/dcm_extractor.py`): REST API with report-based extraction

---

## Appendix C: Common Mistakes to Avoid

### DO NOT DO THIS:

1. **Manual metadata assignment**:
   ```python
   # WRONG
   df['execution_id'] = execution_id
   df['source'] = 'source_name'
   df['extracted_at'] = datetime.now()
   ```

2. **JSON schema files**:
   ```python
   # WRONG
   schema_path = f"shared/schemas/{table_name}.json"
   with open(schema_path) as f:
       schema = json.load(f)
   ```

3. **Missing execution_id parameter**:
   ```python
   # WRONG
   def extract_resource(table_config, config, test_mode=False):
       # Missing execution_id parameter
   ```

4. **Not threading execution_id**:
   ```python
   # WRONG
   def run_pipeline(..., execution_id=None):
       extract_resource(table_config, config)  # Forgot to pass execution_id
   ```

5. **Using print() instead of logger**:
   ```python
   # WRONG
   print("Processing data...")

   # CORRECT
   logger.info("Processing data...")
   ```

6. **Emojis in code**:
   ```python
   # WRONG
   logger.info("✓ Processing complete")

   # CORRECT
   logger.info("Processing complete")
   ```

---

## Appendix D: Validation Scripts

### Validate Metadata Fields

```python
from google.cloud import bigquery

def validate_metadata(source: str, table_name: str):
    """Validate that required metadata fields exist and are populated."""
    client = bigquery.Client(project='bi-data-391216')

    query = f"""
    SELECT
        execution_id,
        source,
        extracted_at,
        COUNT(*) as row_count
    FROM `bi-data-391216.{source}_data.{table_name}`
    GROUP BY execution_id, source, extracted_at
    LIMIT 1
    """

    results = list(client.query(query).result())

    if not results:
        print(f"ERROR: No data in {source}_data.{table_name}")
        return False

    row = results[0]

    if not row.execution_id:
        print(f"ERROR: execution_id is NULL")
        return False

    if not row.source:
        print(f"ERROR: source is NULL")
        return False

    if not row.extracted_at:
        print(f"ERROR: extracted_at is NULL")
        return False

    print(f"SUCCESS: All metadata fields present")
    print(f"  execution_id: {row.execution_id}")
    print(f"  source: {row.source}")
    print(f"  extracted_at: {row.extracted_at}")
    print(f"  row_count: {row.row_count}")

    return True
```

---

## Summary

Follow this specification EXACTLY when creating new pipelines. Any deviation from these patterns must be approved in advance. This ensures consistency, maintainability, and reliability across all data extraction pipelines.

**Key Principles**:
1. Use standard helper functions (never reinvent the wheel)
2. Define schemas in YAML (never JSON)
3. Thread execution_id through all functions
4. Include required metadata fields in all tables
5. Follow existing patterns from reference implementations
6. No emojis, proper comment style, backward compatibility
7. Test thoroughly before deploying to production
