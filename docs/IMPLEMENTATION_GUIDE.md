# Implementation Guide
## Developer Documentation for Orchestrator System

**Last Updated**: 2025-01-21
**Version**: 1.0

---

## Table of Contents
1. [System Architecture](#system-architecture)
2. [Project Structure](#project-structure)
3. [Core Components](#core-components)
4. [Adding New Data Sources](#adding-new-data-sources)
5. [Adding New Tables](#adding-new-tables)
6. [Schema Management](#schema-management)
7. [Testing](#testing)
8. [Deployment](#deployment)
9. [Code Standards](#code-standards)

---

## System Architecture

### Technology Stack

**Languages & Frameworks**:
- Python 3.8+
- SQL (BigQuery dialect)

**Key Dependencies**:
```
facebook-business==21.0.0    # Facebook Graph API SDK
google-cloud-bigquery        # BigQuery client
google-cloud-storage         # GCS client
pandas                       # Data manipulation
pyarrow                      # Parquet I/O
sqlalchemy                   # Database abstraction
pymysql                      # MySQL driver
sshtunnel                    # SSH tunneling
pyyaml                       # Configuration files
```

### Design Patterns

**Plugin Architecture**:
- Each data source is a plugin in `plugins/`
- Plugins implement standard interface
- Orchestrator coordinates execution

**YAML-Driven Configuration**:
- Schema definitions in YAML
- Avoid hardcoding business logic
- Enable configuration changes without code changes

**Data Lake Layers**:
- Raw archive (permanent)
- Staging (latest version)
- Production (optimized analytics)

**Hash-Based Deduplication**:
- SHA256 hash of all columns
- Change detection without column-by-column comparison
- Efficient incremental updates

---

## Project Structure

```
orchestrator/
├── orchestrate.py                  # Main entry point
├── requirements.txt                # Python dependencies
├── .env                            # Environment variables (not in git)
├── env.example                     # Template for .env
│
├── configs/                        # YAML configuration files
│   ├── facebook.yaml               # Facebook source config
│   ├── wordpress.yaml              # WordPress source config
│   ├── defaults.yaml               # Default settings
│   └── alerting.yaml               # Alert configuration
│
├── plugins/                        # Data source plugins
│   ├── facebook_extractor.py       # Facebook Graph API
│   ├── wordpress_extractor.py      # WordPress MySQL/REST
│   └── generic_extractor.py        # Template for new sources
│
├── shared/                         # Shared utilities
│   ├── config_loader.py            # YAML config loading
│   ├── monitoring.py               # Job tracking & alerting
│   ├── bigquery_utils.py           # BigQuery operations
│   ├── gcs_storage.py              # GCS upload/download
│   ├── external_tables.py          # External table creation
│   ├── transform.py                # Data transformation
│   ├── schema_registry.py          # Schema management
│   ├── schema_evolution.py         # Schema evolution
│   ├── schema_discovery.py         # Type discovery (PyArrow/DuckDB)
│   ├── data_validator.py           # Early validation
│   ├── error_handler.py            # Error handling
│   ├── rate_limiter.py             # API rate limiting
│   ├── recovery_cleanup.py         # Cleanup utilities
│   ├── pipeline_validator.py       # Pipeline validation
│   ├── cli_utils.py                # CLI argument parsing
│   ├── log_config.py               # Logging configuration
│   ├── notifications.py            # Email/Slack notifications
│   ├── bigquery_client.py          # BigQuery client factory
│   └── pipeline_utils.py           # Pipeline utilities
│
├── tests/                          # Unit tests (future)
│   ├── test_facebook_extractor.py
│   └── test_wordpress_extractor.py
│
└── docs/                           # Documentation
    ├── DATA_FLOW_MANUAL.md         # Architecture & data flow
    ├── USER_MANUAL.md              # User guide
    ├── IMPLEMENTATION_GUIDE.md     # This file
    └── README.md                   # Project overview
```

---

## Core Components

### 1. Orchestrator (`orchestrate.py`)

**Purpose**: Main entry point, coordinates pipeline execution

**Key Functions**:

```python
def main():
    """Main orchestration function"""
    # 1. Parse arguments
    args = parse_arguments()

    # 2. Load configuration
    config = load_config(args.source)

    # 3. Validate arguments
    validate_arguments(args, config)

    # 4. Initialize monitoring
    job_id = create_centralized_job(...)

    # 5. Run extraction plugin
    results = run_extraction(args, config)

    # 6. Upload to GCS
    gcs_paths = upload_to_gcs(results, config)

    # 7. Create external tables
    external_tables = create_external_tables(gcs_paths, config)

    # 8. Transform to production
    transform_results = transform_to_production(external_tables, config)

    # 9. Update monitoring
    complete_execution(job_id, results)
```

**Error Handling**:
```python
try:
    # Execute pipeline
    results = run_pipeline(...)
except Exception as e:
    # Log error
    logger.error(f"Pipeline failed: {e}")

    # Update monitoring
    fail_execution(job_id, str(e))

    # Send alert
    send_job_failure_alert(job_id, str(e))

    # Exit with error code
    sys.exit(1)
```

### 2. Extractor Plugins

**Interface Contract**:

Every extractor must implement:

```python
def run_pipeline(
    source: str,
    config: Dict[str, Any],
    sites: List[str],
    tables: List[str],
    refresh_mode: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Extract data from source.

    Args:
        source: Source name (e.g., 'facebook')
        config: YAML configuration dict
        sites: List of site/account identifiers
        tables: List of tables to extract
        refresh_mode: 'incremental' or 'full'
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)

    Returns:
        {
            'total_rows': int,           # Total rows extracted
            'table_files': {             # Parquet files created
                'table_name': '/path/to/file.parquet'
            },
            'extraction_stats': {        # Per-table statistics
                'table_name': {
                    'rows': int,
                    'sites': List[str],
                    'error': Optional[str]
                }
            }
        }
    """
    pass
```

**Example Implementation**:

```python
# plugins/facebook_extractor.py

def run_pipeline(source, config, sites, tables, refresh_mode, **kwargs):
    results = {
        'total_rows': 0,
        'table_files': {},
        'extraction_stats': {}
    }

    # Create temp directory
    temp_dir = tempfile.mkdtemp(prefix='orchestrator_')

    # Extract each table
    for table in tables:
        try:
            # Extract data
            data = extract_table(table, sites, config, refresh_mode)

            # Save to Parquet
            file_path = os.path.join(temp_dir, f"{table}.parquet")
            df = pd.DataFrame(data)
            df.to_parquet(file_path, index=False)

            # Update results
            results['total_rows'] += len(data)
            results['table_files'][table] = file_path
            results['extraction_stats'][table] = {
                'rows': len(data),
                'sites': sites
            }

        except Exception as e:
            logger.error(f"Failed to extract {table}: {e}")
            results['extraction_stats'][table] = {
                'rows': 0,
                'error': str(e)
            }

    return results
```

### 3. Configuration Loader (`shared/config_loader.py`)

**Purpose**: Load and merge YAML configurations

**Key Functions**:

```python
def load_config(source: str) -> Dict[str, Any]:
    """
    Load configuration for a source.

    Merges:
    1. defaults.yaml (base settings)
    2. {source}.yaml (source-specific)
    3. Environment variables (overrides)

    Args:
        source: Source name (e.g., 'facebook')

    Returns:
        Merged configuration dictionary
    """
    # Load defaults
    defaults = load_yaml('configs/defaults.yaml')

    # Load source config
    source_config = load_yaml(f'configs/{source}.yaml')

    # Merge configurations
    config = deep_merge(defaults, source_config)

    # Substitute environment variables
    config = substitute_env_vars(config)

    return config
```

**Environment Variable Substitution**:

```yaml
# configs/facebook.yaml
source:
  api_token: ${FACEBOOK_ACCESS_TOKEN}  # From .env

pipeline:
  gcs_bucket: ${GCS_BUCKET}            # From .env
```

### 4. Monitoring (`shared/monitoring.py`)

**Purpose**: Track job execution and send alerts

**Key Functions**:

```python
def create_centralized_job(
    source: str,
    job_type: str,
    environment: str,
    tables: List[str],
    bq_client: bigquery.Client
) -> str:
    """
    Create job record in orchestrator_monitoring.jobs

    Returns:
        job_id (UUID)
    """
    job_id = str(uuid.uuid4())

    job_data = {
        'job_id': job_id,
        'source': source,
        'job_type': job_type,
        'environment': environment,
        'tables': ','.join(tables),
        'status': 'running',
        'created_at': datetime.now(timezone.utc)
    }

    # Insert to BigQuery
    bq_client.insert_rows_json(
        'orchestrator_monitoring.jobs',
        [job_data]
    )

    return job_id


def complete_execution(
    job_id: str,
    results: Dict[str, Any],
    bq_client: bigquery.Client
):
    """Update job status to completed"""
    update_query = f"""
    UPDATE orchestrator_monitoring.jobs
    SET
        status = 'completed',
        completed_at = CURRENT_TIMESTAMP(),
        execution_time_seconds = TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), created_at, SECOND),
        rows_extracted = {results['total_rows']},
        rows_inserted = {results.get('rows_inserted', 0)},
        rows_updated = {results.get('rows_updated', 0)}
    WHERE job_id = '{job_id}'
    """
    bq_client.query(update_query).result()
```

### 5. Schema Management (`shared/schema_registry.py`)

**Purpose**: Centralized schema definitions

**Key Functions**:

```python
def get_schema(
    source: str,
    table: str,
    include_etl_fields: bool = True,
    use_cache: bool = True
) -> Dict[str, str]:
    """
    Get schema for a table from YAML config.

    Args:
        source: Source name
        table: Table name
        include_etl_fields: Add ETL fields (row_hash, extracted_at, etc.)
        use_cache: Use in-memory cache

    Returns:
        {column_name: bigquery_type}
    """
    # Load from YAML
    config = load_config(source)
    schema = config.get('resources', {}).get(table, {}).get('schema', {})

    # Add ETL fields
    if include_etl_fields:
        schema.update({
            'row_hash': 'STRING',
            'extracted_at': 'TIMESTAMP',
            'updated_at': 'TIMESTAMP'
        })

    return schema


def get_bq_schema(source: str, table: str) -> List[bigquery.SchemaField]:
    """
    Get BigQuery SchemaField list for table creation.

    Returns:
        [SchemaField(...), ...]
    """
    schema_dict = get_schema(source, table)

    bq_fields = []
    for col_name, col_type in schema_dict.items():
        bq_fields.append(
            bigquery.SchemaField(col_name, col_type, mode='NULLABLE')
        )

    return bq_fields
```

### 6. BigQuery Utilities (`shared/bigquery_utils.py`)

**Purpose**: BigQuery operations (MERGE, schema evolution, etc.)

**Key Functions**:

```python
def process_hash_merge(
    bq_client: bigquery.Client,
    source_table_id: str,
    target_table_id: str,
    primary_keys: List[str],
    schema: Dict[str, str],
    rebuild_mode: bool = False
) -> Dict[str, int]:
    """
    Execute hash-based MERGE from source to target.

    Args:
        bq_client: BigQuery client
        source_table_id: External table (source)
        target_table_id: Production table (target)
        primary_keys: Primary key columns
        schema: Expected schema
        rebuild_mode: Drop and recreate target table

    Returns:
        {
            'rows_inserted': int,
            'rows_updated': int,
            'rows_deleted': int
        }
    """
    # Handle rebuild mode
    if rebuild_mode:
        logger.warning(f"REBUILD MODE: Dropping table {target_table_id}")
        bq_client.delete_table(target_table_id, not_found_ok=True)

    # Create target table if not exists
    if not table_exists(bq_client, target_table_id):
        create_table_from_schema(bq_client, target_table_id, schema)

    # Apply schema evolution
    from shared.schema_evolution import apply_schema_evolution
    apply_schema_evolution(bq_client, source_table_id, target_table_id)

    # Build MERGE query
    merge_query = build_merge_query(
        source_table_id,
        target_table_id,
        primary_keys,
        schema
    )

    # Execute MERGE
    job = bq_client.query(merge_query)
    job.result()

    # Get statistics
    stats = get_merge_statistics(bq_client, target_table_id)

    return stats


def build_merge_query(
    source_table_id: str,
    target_table_id: str,
    primary_keys: List[str],
    schema: Dict[str, str]
) -> str:
    """Build MERGE SQL query"""

    # Build column list (exclude ETL fields from source)
    data_columns = [col for col in schema.keys()
                    if col not in ['row_hash', 'updated_at', 'extracted_at']]

    # Build primary key join condition
    pk_condition = ' AND '.join([
        f'target.{pk} = source.{pk}' for pk in primary_keys
    ])

    # Build UPDATE SET clause
    update_columns = [col for col in data_columns if col not in primary_keys]
    update_set = ', '.join([
        f'target.{col} = source.{col}' for col in update_columns
    ])

    # Build hash calculation
    hash_columns = ', '.join([f'source.{col}' for col in data_columns])

    query = f"""
    MERGE `{target_table_id}` AS target
    USING (
        SELECT
            *,
            TO_HEX(SHA256(TO_JSON_STRING(STRUCT({hash_columns})))) as row_hash,
            CURRENT_TIMESTAMP() as extracted_at
        FROM `{source_table_id}`
    ) AS source
    ON {pk_condition}

    -- Insert new rows
    WHEN NOT MATCHED THEN
        INSERT ({', '.join(data_columns + ['row_hash', 'extracted_at', 'updated_at'])})
        VALUES ({', '.join(['source.' + col for col in data_columns])},
                source.row_hash,
                source.extracted_at,
                CURRENT_TIMESTAMP())

    -- Update changed rows
    WHEN MATCHED AND target.row_hash != source.row_hash THEN
        UPDATE SET
            {update_set},
            row_hash = source.row_hash,
            updated_at = CURRENT_TIMESTAMP()
    """

    return query
```

---

## Adding New Data Sources

### Step 1: Create Extractor Plugin

**File**: `plugins/{source}_extractor.py`

```python
#!/usr/bin/env python3
"""
{Source} Data Extractor
Extracts data from {source} and saves to Parquet files.
"""

import logging
import tempfile
import os
import pandas as pd
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


def run_pipeline(
    source: str,
    config: Dict[str, Any],
    sites: List[str],
    tables: List[str],
    refresh_mode: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Main extraction pipeline for {source}.

    Args:
        source: Source name
        config: YAML configuration
        sites: List of sites/accounts to extract
        tables: List of tables to extract
        refresh_mode: 'incremental' or 'full'
        start_date: Optional start date
        end_date: Optional end date

    Returns:
        {
            'total_rows': int,
            'table_files': {table_name: file_path},
            'extraction_stats': {table_name: {rows, sites}}
        }
    """
    logger.info(f"Starting {source} extraction")
    logger.info(f"  Tables: {tables}")
    logger.info(f"  Sites: {sites}")
    logger.info(f"  Mode: {refresh_mode}")

    results = {
        'total_rows': 0,
        'table_files': {},
        'extraction_stats': {}
    }

    # Create temp directory for Parquet files
    temp_dir = tempfile.mkdtemp(prefix=f'orchestrator_{source}_')
    logger.info(f"Temp directory: {temp_dir}")

    # Extract each table
    for table in tables:
        try:
            logger.info(f"\nExtracting {table}...")

            # Extract data from API/database
            data = extract_table(table, sites, config, refresh_mode, start_date, end_date)

            if not data:
                logger.warning(f"No data extracted for {table}")
                results['extraction_stats'][table] = {
                    'rows': 0,
                    'sites': sites,
                    'error': None
                }
                continue

            # Convert to DataFrame
            df = pd.DataFrame(data)

            # Save to Parquet
            file_path = os.path.join(temp_dir, f"{table}.parquet")
            df.to_parquet(file_path, index=False, engine='pyarrow')

            logger.info(f"  Saved {len(data)} rows to {file_path}")

            # Update results
            results['total_rows'] += len(data)
            results['table_files'][table] = file_path
            results['extraction_stats'][table] = {
                'rows': len(data),
                'sites': sites,
                'error': None
            }

        except Exception as e:
            logger.error(f"Failed to extract {table}: {e}")
            results['extraction_stats'][table] = {
                'rows': 0,
                'sites': sites,
                'error': str(e)
            }

    logger.info(f"\nExtraction complete: {results['total_rows']} total rows")
    return results


def extract_table(
    table: str,
    sites: List[str],
    config: Dict[str, Any],
    refresh_mode: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Extract a specific table from {source}.

    Returns:
        List of dictionaries (one per row)
    """
    # Get table configuration
    table_config = config.get('resources', {}).get(table, {})

    # Initialize API client
    client = initialize_client(config)

    # Determine date range
    if refresh_mode == 'incremental':
        # Get last extraction time
        from shared.monitoring import get_last_extraction_time
        last_run = get_last_extraction_time('source', table)

        # Apply lookback
        lookback_days = table_config.get('incremental', {}).get('lookback_days', 7)
        start_date = (last_run - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

    # Extract from each site
    all_data = []
    for site in sites:
        try:
            site_data = extract_from_site(client, table, site, start_date, end_date, table_config)
            all_data.extend(site_data)
        except Exception as e:
            logger.error(f"Failed to extract {table} from {site}: {e}")

    return all_data


def initialize_client(config: Dict[str, Any]):
    """Initialize API client"""
    api_key = config.get('source', {}).get('api_key')
    # Initialize and return client
    pass


def extract_from_site(client, table, site, start_date, end_date, config):
    """Extract data from a specific site"""
    # Implement extraction logic
    pass
```

### Step 2: Create YAML Configuration

**File**: `configs/{source}.yaml`

```yaml
# {Source} Data Source Configuration

source:
  type: {source}
  api_endpoint: https://api.example.com
  api_key: ${SOURCE_API_KEY}  # From .env
  api_version: v1

  # Default sites (can be overridden with --sites)
  sites:
    - site1
    - site2

pipeline:
  staging_dataset: {source}_staging
  production_dataset: {source}_data
  gcs_bucket: ${GCS_BUCKET}

# Table/resource definitions
resources:
  table1:
    table_name: {source}_table1
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
      status: STRING
      count: INT64
      amount: FLOAT64

  table2:
    table_name: {source}_table2
    primary_key: [id, site_id]

    incremental:
      mode: cursor
      cursor_field: modified_date

    schema:
      id: STRING
      site_id: STRING
      title: STRING
      modified_date: TIMESTAMP
```

### Step 3: Register in Orchestrator

**File**: `orchestrate.py`

```python
# Import new extractor
from plugins.{source}_extractor import run_pipeline as {source}_run_pipeline

# Add to extractor mapping
EXTRACTORS = {
    'facebook': facebook_run_pipeline,
    'wordpress': wordpress_run_pipeline,
    '{source}': {source}_run_pipeline,  # Add new source
}

# In main function
extractor = EXTRACTORS.get(args.source)
if not extractor:
    raise ValueError(f"Unknown source: {args.source}")

results = extractor(
    source=args.source,
    config=config,
    sites=sites,
    tables=tables,
    refresh_mode=args.refresh,
    start_date=args.start_date,
    end_date=args.end_date
)
```

### Step 4: Add Environment Variables

**File**: `.env`

```bash
# {Source} API credentials
SOURCE_API_KEY=your_api_key_here
SOURCE_API_SECRET=your_api_secret_here
```

### Step 5: Test New Source

```bash
# Test extraction only
python orchestrate.py \
  --source {source} \
  --tables table1 \
  --sites site1 \
  --extract-only \
  --keep-local \
  --log-level debug

# Full pipeline test
python orchestrate.py \
  --source {source} \
  --tables table1 \
  --sites site1 \
  --refresh full \
  --rebuild
```

---

## Adding New Tables

### Step 1: Add to YAML Configuration

**File**: `configs/{source}.yaml`

```yaml
resources:
  new_table:
    table_name: {source}_new_table
    primary_key: [id]  # Primary key columns

    incremental:
      mode: lookback
      lookback_days: 7
      cursor_field: updated_at  # Column for incremental filtering

    # Define schema
    schema:
      id: STRING
      name: STRING
      description: STRING
      created_time: TIMESTAMP
      updated_time: TIMESTAMP
      status: STRING
      metrics: STRING  # JSON string for complex data
```

### Step 2: Update Extractor Logic

**File**: `plugins/{source}_extractor.py`

```python
def extract_table(table, sites, config, refresh_mode, start_date, end_date):
    """Extract specific table"""

    # Add handling for new table
    if table == 'new_table':
        return extract_new_table(sites, config, start_date, end_date)

    # ... existing table handlers


def extract_new_table(sites, config, start_date, end_date):
    """Extract new_table data"""
    all_data = []

    for site in sites:
        # Call API
        response = api_client.get_new_table(
            site_id=site,
            since=start_date,
            until=end_date
        )

        # Transform to flat structure
        for item in response:
            record = {
                'id': item['id'],
                'name': item['name'],
                'description': item.get('description'),
                'created_time': item['created_time'],
                'updated_time': item.get('updated_time'),
                'status': item.get('status'),
                'metrics': json.dumps(item.get('metrics', {}))  # Flatten complex data
            }
            all_data.append(record)

    return all_data
```

### Step 3: First Extraction (Rebuild Mode)

```bash
# Extract with schema discovery and table creation
python orchestrate.py \
  --source {source} \
  --tables new_table \
  --refresh full \
  --rebuild \
  --log-level debug
```

**What this does**:
1. Extracts all data from new_table
2. Discovers optimal schema using PyArrow/DuckDB
3. Creates production table in BigQuery
4. Updates YAML config with discovered schema

### Step 4: Verify Data

```sql
-- Check row count
SELECT COUNT(*) FROM {source}_data.new_table;

-- Check schema
SELECT column_name, data_type
FROM `bi-data-391216.{source}_data`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'new_table'
ORDER BY ordinal_position;

-- Sample data
SELECT * FROM {source}_data.new_table LIMIT 10;
```

### Step 5: Add to Daily Schedule

```bash
# Add to crontab
crontab -e

# Add line
0 6 * * * cd /path/to/orchestrator && python orchestrate.py --source {source} --tables existing_table new_table >> /var/log/{source}_daily.log 2>&1
```

---

## Schema Management

### Schema Priority

1. **YAML Config** (Primary)
2. **Production Table** (Fallback)
3. **Runtime Discovery** (Last Resort)

### Defining Schemas in YAML

```yaml
resources:
  campaigns:
    schema:
      # String columns
      id: STRING
      name: STRING
      status: STRING

      # Numeric columns
      daily_budget: INT64
      lifetime_budget: INT64
      bid_amount: FLOAT64

      # Timestamp columns
      created_time: TIMESTAMP
      updated_time: TIMESTAMP

      # Boolean columns
      is_active: BOOLEAN

      # Complex data (JSON strings)
      targeting: STRING  # JSON string
      optimization_goal: STRING
```

### Schema Evolution

**Automatic** (handled by `shared/schema_evolution.py`):

```python
def apply_schema_evolution(
    bq_client: bigquery.Client,
    source_table_id: str,
    target_table_id: str
):
    """
    Compare schemas and add new columns to target.
    """
    # Get schemas
    source_table = bq_client.get_table(source_table_id)
    target_table = bq_client.get_table(target_table_id)

    source_cols = {f.name: f.field_type for f in source_table.schema}
    target_cols = {f.name: f.field_type for f in target_table.schema}

    # Find new columns
    new_columns = set(source_cols.keys()) - set(target_cols.keys())

    # Add each new column
    for col_name in new_columns:
        col_type = source_cols[col_name]

        logger.info(f"Adding column {col_name} ({col_type}) to {target_table_id}")

        alter_query = f"""
        ALTER TABLE `{target_table_id}`
        ADD COLUMN IF NOT EXISTS {col_name} {col_type}
        """
        bq_client.query(alter_query).result()
```

### Type Conversion

**Safe casting in MERGE**:

```sql
-- Automatic safe casting for type mismatches
SAFE_CAST(source.daily_budget AS INT64) AS daily_budget,
SAFE_CAST(source.created_time AS TIMESTAMP) AS created_time
```

**Handling mixed types** (Facebook actions):

```python
# Force problematic columns to strings
problematic_patterns = ['action', 'cost_per', 'conversion']

for col in df.columns:
    if any(pattern in col.lower() for pattern in problematic_patterns):
        df[col] = df[col].apply(lambda x: json.dumps(x) if x else None)
```

---

## Testing

### Unit Tests

**File**: `tests/test_{source}_extractor.py`

```python
import unittest
from plugins.{source}_extractor import extract_table
from shared.config_loader import load_config


class Test{Source}Extractor(unittest.TestCase):

    def setUp(self):
        """Load test configuration"""
        self.config = load_config('{source}')

    def test_extract_table(self):
        """Test table extraction"""
        data = extract_table(
            table='table1',
            sites=['test_site'],
            config=self.config,
            refresh_mode='full',
            start_date='2025-01-01',
            end_date='2025-01-21'
        )

        # Assert data structure
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)

        # Assert required fields
        first_row = data[0]
        self.assertIn('id', first_row)
        self.assertIn('name', first_row)

    def test_incremental_mode(self):
        """Test incremental extraction"""
        # Test implementation
        pass


if __name__ == '__main__':
    unittest.main()
```

### Integration Tests

```bash
# Test extraction only (no BigQuery changes)
python orchestrate.py \
  --source {source} \
  --tables table1 \
  --sites test_site \
  --extract-only \
  --keep-local

# Verify Parquet file
python -c "
import pandas as pd
df = pd.read_parquet('/tmp/orchestrator_*/table1.parquet')
print(df.info())
print(df.head())
"

# Test full pipeline with staging environment
python orchestrate.py \
  --source {source} \
  --tables table1 \
  --sites test_site \
  --env staging \
  --refresh full \
  --rebuild
```

### Manual Testing Checklist

- [ ] Extraction produces correct row count
- [ ] Parquet file has expected schema
- [ ] GCS upload succeeds (raw + staging)
- [ ] External table created successfully
- [ ] MERGE inserts/updates correct rows
- [ ] Schema evolution adds new columns
- [ ] Job monitoring records correct stats
- [ ] Incremental mode uses correct lookback
- [ ] Error handling and retry work
- [ ] Logs are clear and informative

---

## Deployment

### Production Deployment

**Step 1: Set up environment**

```bash
# Create .env file
cp env.example .env

# Edit .env with production credentials
vim .env
```

**Step 2: Install dependencies**

```bash
pip install -r requirements.txt
```

**Step 3: Verify Google Cloud access**

```bash
# Authenticate
gcloud auth application-default login

# Set project
gcloud config set project bi-data-391216

# Test BigQuery access
bq ls
```

**Step 4: Create datasets**

```bash
# Create staging datasets
bq mk --dataset bi-data-391216:facebook_staging
bq mk --dataset bi-data-391216:wordpress_staging

# Create production datasets
bq mk --dataset bi-data-391216:facebook_data
bq mk --dataset bi-data-391216:wordpress_data

# Create monitoring dataset
bq mk --dataset bi-data-391216:orchestrator_monitoring
```

**Step 5: Create GCS bucket**

```bash
gsutil mb -p bi-data-391216 -l US gs://orchestrator-data-lake
```

**Step 6: Set up cron jobs**

```bash
crontab -e
```

```cron
# Facebook daily update (6 AM)
0 6 * * * cd /path/to/orchestrator && python orchestrate.py --source facebook --tables campaigns adsets ads >> /var/log/facebook_daily.log 2>&1

# WordPress daily update (7 AM)
0 7 * * * cd /path/to/orchestrator && python orchestrate.py --source wordpress --tables posts postmeta >> /var/log/wordpress_daily.log 2>&1

# Facebook weekly full refresh (Sunday 2 AM)
0 2 * * 0 cd /path/to/orchestrator && python orchestrate.py --source facebook --tables campaigns adsets ads --refresh full >> /var/log/facebook_weekly.log 2>&1
```

### Monitoring Setup

**Email alerts** (`configs/alerting.yaml`):

```yaml
email:
  enabled: true
  smtp_host: ${SMTP_HOST}
  smtp_port: 587
  smtp_user: ${SMTP_USER}
  smtp_password: ${SMTP_PASSWORD}
  recipients:
    - data-team@company.com

alerts:
  job_failure: true
  job_success: false
  long_running_job: true
  threshold_hours: 2
```

### Log Rotation

```bash
# Create logrotate config
sudo vim /etc/logrotate.d/orchestrator
```

```
/var/log/facebook_*.log /var/log/wordpress_*.log {
    daily
    rotate 30
    compress
    delaycompress
    notifempty
    create 0644 user group
}
```

---

## Code Standards

### Python Style

**Follow PEP 8**:
- 4 spaces for indentation
- Max line length: 100 characters
- Snake_case for functions/variables
- PascalCase for classes

**Imports**:
```python
# Standard library
import os
import sys
import logging

# Third-party
import pandas as pd
from google.cloud import bigquery

# Local
from shared.config_loader import load_config
from shared.monitoring import create_centralized_job
```

### Documentation

**Module docstrings**:
```python
#!/usr/bin/env python3
"""
Facebook Data Extractor
=======================

Extracts campaigns, adsets, ads, and insights from Facebook Graph API.

Author: Data Team
Created: 2025-01-21
"""
```

**Function docstrings**:
```python
def extract_table(table: str, sites: List[str], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract data from a specific table.

    Args:
        table: Table name (e.g., 'campaigns')
        sites: List of site/account identifiers
        config: YAML configuration dictionary

    Returns:
        List of dictionaries, one per row

    Raises:
        ValueError: If table is not configured
        ConnectionError: If API is unreachable

    Example:
        >>> config = load_config('facebook')
        >>> data = extract_table('campaigns', ['act_123456'], config)
        >>> len(data)
        1234
    """
```

### Logging

**Use structured logging**:

```python
import logging

logger = logging.getLogger(__name__)

# Info level for normal operations
logger.info(f"Extracting {table} from {len(sites)} sites")

# Debug level for detailed info
logger.debug(f"API request: {api_url}")

# Warning for recoverable issues
logger.warning(f"No data found for {site}, skipping")

# Error for failures
logger.error(f"Failed to extract {table}: {e}")
```

### Error Handling

**Specific exception handling**:

```python
try:
    data = api_client.get_campaigns()
except RateLimitError as e:
    logger.warning(f"Rate limit hit, retrying after {e.retry_after}s")
    time.sleep(e.retry_after)
    data = api_client.get_campaigns()
except AuthenticationError as e:
    logger.error(f"Authentication failed: {e}")
    raise
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    raise
```

### Configuration

**Use YAML for configuration**:
- No hardcoded values in code
- Environment variables for secrets
- Schema definitions in YAML

**Bad**:
```python
# Hardcoded - BAD
api_endpoint = "https://graph.facebook.com/v21.0"
lookback_days = 7
```

**Good**:
```python
# From config - GOOD
api_endpoint = config.get('source', {}).get('api_endpoint')
lookback_days = config.get('resources', {}).get(table, {}).get('incremental', {}).get('lookback_days', 7)
```

---

**End of Implementation Guide**

For architecture details, see [DATA_FLOW_MANUAL.md](DATA_FLOW_MANUAL.md)
For user guide, see [USER_MANUAL.md](USER_MANUAL.md)
