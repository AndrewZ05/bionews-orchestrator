# Data Flow Manual
## Complete Architecture from Source to Analytics

**Last Updated**: 2025-01-21
**Version**: 1.0

---

## Table of Contents
1. [Overview](#overview)
2. [Architecture Diagram](#architecture-diagram)
3. [Data Sources](#data-sources)
4. [Pipeline Stages](#pipeline-stages)
5. [Data Storage Layers](#data-storage-layers)
6. [Schema Management](#schema-management)
7. [Monitoring & Recovery](#monitoring--recovery)

---

## Overview

The Orchestrator system is a production-grade data pipeline that extracts data from multiple sources (Facebook, WordPress) and loads it into BigQuery for analytics. The system implements a **data lake architecture** with multiple storage layers, comprehensive monitoring, and automatic recovery.

### Key Features
- **Multi-source extraction**: Facebook Graph API, WordPress MySQL/REST API
- **Data lake architecture**: Raw archive + staging + production layers
- **Schema evolution**: Automatic type discovery and YAML-driven schema management
- **Incremental updates**: Hash-based deduplication with lookback windows
- **Monitoring**: Job tracking, alerting, and recovery capabilities
- **Type safety**: Robust Parquet conversion with mixed-type handling

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────────┐              ┌────────────────────┐          │
│  │  Facebook Graph  │              │  WordPress Sites   │          │
│  │      API         │              │  (MySQL + REST)    │          │
│  └────────┬─────────┘              └──────────┬─────────┘          │
│           │                                     │                    │
└───────────┼─────────────────────────────────────┼────────────────────┘
            │                                     │
            ▼                                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    EXTRACTION LAYER (Python)                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────────────────────────────────────────────┐          │
│  │  orchestrate.py (Main Controller)                    │          │
│  │  - Argument parsing & validation                     │          │
│  │  - Job tracking & monitoring                         │          │
│  │  - Error handling & recovery                         │          │
│  └────────────┬─────────────────────────────────────────┘          │
│               │                                                      │
│               ├─────────────┬────────────────────┐                 │
│               ▼             ▼                    ▼                  │
│  ┌───────────────┐  ┌──────────────┐  ┌─────────────────┐         │
│  │   Facebook    │  │  WordPress   │  │    Generic      │         │
│  │  Extractor    │  │  Extractor   │  │   Extractor     │         │
│  │               │  │              │  │  (Future API)   │         │
│  └───────┬───────┘  └──────┬───────┘  └─────────────────┘         │
│          │                  │                                        │
│          │ Parquet Files    │ Parquet Files                         │
│          │                  │                                        │
└──────────┼──────────────────┼────────────────────────────────────────┘
           │                  │
           ▼                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      LOCAL STORAGE (Temporary)                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  /tmp/orchestrator_{job_id}/                                        │
│    └── {table}.parquet                                              │
│                                                                      │
│  Purpose: Temporary staging for type conversion & validation        │
│  Retention: Deleted after successful upload (configurable)          │
│                                                                      │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           │ Upload
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    GOOGLE CLOUD STORAGE (Data Lake)                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  gs://{GCS_BUCKET}/                                                 │
│                                                                      │
│  ┌──────────────────────────────────────────────────────┐          │
│  │  RAW ARCHIVE (Permanent Historical Data)             │          │
│  │  ────────────────────────────────────────             │          │
│  │  {source}/raw/{resource}/{YYYY}/{MM}/{DD}/           │          │
│  │    └── {execution_id}_{timestamp}.parquet            │          │
│  │                                                       │          │
│  │  Example:                                             │          │
│  │  facebook/raw/campaigns/2025/01/21/                   │          │
│  │    └── exec_001_20250121T143022.parquet              │          │
│  │                                                       │          │
│  │  Purpose: Complete historical archive                │          │
│  │  Retention: Permanent (or GCS lifecycle policy)      │          │
│  │  Format: Parquet with schema enforcement             │          │
│  └──────────────────────────────────────────────────────┘          │
│                                                                      │
│  ┌──────────────────────────────────────────────────────┐          │
│  │  STAGING (Latest Version for External Tables)        │          │
│  │  ───────────────────────────────────────────          │          │
│  │  {source}/staging/{resource}/latest/                 │          │
│  │    └── data.parquet                                  │          │
│  │                                                       │          │
│  │  Example:                                             │          │
│  │  facebook/staging/campaigns/latest/data.parquet       │          │
│  │                                                       │          │
│  │  Purpose: Current version for BigQuery external table│          │
│  │  Retention: Overwritten each run                     │          │
│  │  Format: Parquet (same as raw archive)               │          │
│  └──────────────────────────────────────────────────────┘          │
│                                                                      │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           │ Create External Table
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        BIGQUERY (Analytics)                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  PROJECT: bi-data-391216                                            │
│                                                                      │
│  ┌──────────────────────────────────────────────────────┐          │
│  │  STAGING DATASET: {source}_staging                   │          │
│  │  ──────────────────────────────────                  │          │
│  │  External Tables (point to GCS staging/)             │          │
│  │                                                       │          │
│  │  - campaigns_external                                │          │
│  │  - posts_external                                    │          │
│  │  - ...                                               │          │
│  │                                                       │          │
│  │  Purpose: BigQuery view of latest GCS data           │          │
│  │  Retention: Recreated each run                       │          │
│  │  Schema: Auto-detected from Parquet                  │          │
│  └──────────────────────────────────────────────────────┘          │
│                           │                                          │
│                           │ Hash-based MERGE                         │
│                           ▼                                          │
│  ┌──────────────────────────────────────────────────────┐          │
│  │  PRODUCTION DATASET: {source}_data                   │          │
│  │  ───────────────────────────────────                 │          │
│  │  Native BigQuery Tables (optimized storage)          │          │
│  │                                                       │          │
│  │  - campaigns                                         │          │
│  │  - posts                                             │          │
│  │  - page_insights                                     │          │
│  │  - ...                                               │          │
│  │                                                       │          │
│  │  Purpose: Production analytics tables                │          │
│  │  Retention: Permanent                                │          │
│  │  Schema: Managed via YAML + evolution                │          │
│  │  Updates: Hash-based deduplication (no duplicates)   │          │
│  └──────────────────────────────────────────────────────┘          │
│                                                                      │
│  ┌──────────────────────────────────────────────────────┐          │
│  │  MONITORING DATASET: orchestrator_monitoring          │          │
│  │  ──────────────────────────────────────────           │          │
│  │  - jobs (execution tracking)                         │          │
│  │  - extraction_state (incremental cursors)            │          │
│  │                                                       │          │
│  │  Purpose: Job history & incremental state            │          │
│  │  Retention: Permanent                                │          │
│  └──────────────────────────────────────────────────────┘          │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Data Sources

### Facebook Graph API

**Extractor**: `plugins/facebook_extractor.py`

**Authentication**:
- Access Token (long-lived user token or system user token)
- App ID + App Secret (for token refresh)

**Entities**:
- **Ad Accounts** (`act_123456`): Campaigns, AdSets, Ads, Insights
- **Pages** (`123456789`): Posts, Page Insights, Post Insights

**API Endpoints**:
```
Graph API v21.0
├── /{ad_account_id}/campaigns
├── /{ad_account_id}/adsets
├── /{ad_account_id}/ads
├── /{ad_account_id}/insights (campaign/adset/ad level)
├── /{page_id}/posts
├── /{page_id}/insights
└── /{post_id}/insights
```

**Rate Limits**:
- Error 4: App-level rate limit (1min, 3min, 5min backoff)
- Error 17: User request limit (3min, 10min, 20min backoff)
- Error 80004: Account-level limit (5min, 15min, 30min, 60min backoff)

**Data Characteristics**:
- **Nested JSON**: Metrics returned as arrays of objects
- **Mixed types**: Action columns can be int or string
- **Date ranges**: Insights require date chunking (89-day max)
- **Pagination**: Cursor-based (after/before)

### WordPress Sites

**Extractor**: `plugins/wordpress_extractor.py`

**Authentication**:
- SSH tunnel to MySQL database
- WordPress REST API (for media/metadata)

**Connection Flow**:
1. Fetch site list from Sites API
2. For each site:
   - Create SSH tunnel to MySQL server
   - Extract from `wp_posts`, `wp_postmeta`, `wp_users`, etc.
   - Close tunnel

**Tables**:
```
WordPress MySQL Database
├── wp_posts (articles, pages)
├── wp_postmeta (custom fields)
├── wp_users (authors)
├── wp_comments
└── wp_options (site settings)
```

**Data Characteristics**:
- **Relational**: Standard MySQL schema
- **Metadata**: Serialized PHP in postmeta
- **Large tables**: Posts can have millions of rows
- **Incremental**: post_modified timestamp for lookback

---

## Pipeline Stages

### Stage 1: Extraction

**File**: `orchestrate.py` (lines 700-838)

**Process**:
1. Load YAML configuration
2. Validate arguments (source, tables, sites)
3. Call extractor plugin
4. Extractor returns: `{ 'total_rows': int, 'table_files': { table: path } }`

**Output**: Local Parquet files in `/tmp/orchestrator_{job_id}/`

**Error Handling**:
- API rate limits: Automatic retry with backoff
- Network errors: Transient error retry (3 attempts)
- Schema errors: Type conversion with fallback to strings

### Stage 2: GCS Upload

**File**: `orchestrate.py` (lines 839-876)

**Process**:
1. Get GCS bucket name from config
2. Verify bucket access (test write/read/delete)
3. For each extracted table:
   - Upload to **RAW path** (date-partitioned)
   - Upload to **STAGING path** (latest version)
4. Store GCS paths for next stage

**Output**: Files in GCS at two locations:
```
gs://{bucket}/facebook/raw/campaigns/2025/01/21/exec_001_timestamp.parquet
gs://{bucket}/facebook/staging/campaigns/latest/data.parquet
```

**Skip Conditions**:
- `--skip-gcs`: Skip GCS upload entirely
- `--extract-only`: Extract to local only
- No GCS_BUCKET configured: Skip with warning

### Stage 3: External Tables

**File**: `orchestrate.py` (lines 878-914)

**Process**:
1. For each GCS staging file:
   - Create external table in `{source}_staging` dataset
   - Name: `{table}_external`
   - Format: Parquet with auto-schema detection
2. Verify row count matches extraction

**Output**: BigQuery external tables pointing to GCS:
```sql
CREATE EXTERNAL TABLE facebook_staging.campaigns_external
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://bucket/facebook/staging/campaigns/latest/data.parquet']
)
```

**Skip Conditions**:
- `--skip-external-tables`: Skip external table creation
- `--skip-gcs`: Also skips external tables

### Stage 4: Transform to Production

**File**: `orchestrate.py` (lines 916-1068)

**Process**:
1. Get primary keys for table (from YAML or defaults)
2. Check if production table exists
3. Execute hash-based MERGE:
   ```sql
   MERGE {source}_data.{table} AS target
   USING {source}_staging.{table}_external AS source
   ON target.id = source.id AND target.account_id = source.account_id
   WHEN NOT MATCHED THEN INSERT
   WHEN MATCHED AND target.row_hash != source.row_hash THEN UPDATE
   ```
4. Apply schema evolution (add new columns)
5. Log inserted/updated/deleted counts

**Output**: Production table with deduplicated data

**Merge Logic**:
- **INSERT**: New records (primary key doesn't exist)
- **UPDATE**: Changed records (hash mismatch)
- **NO ACTION**: Unchanged records (hash match)
- **DELETE**: Never (incremental only adds/updates)

**Schema Evolution**:
- New columns automatically added as NULLABLE
- Type mismatches: Use SAFE_CAST with NULL fallback
- Missing columns: Filled with NULL

### Stage 5: Monitoring & Cleanup

**File**: `orchestrate.py` (lines 1070-1120)

**Process**:
1. Update job status in `orchestrator_monitoring.jobs`
2. Store extraction state for incremental (last_extracted_at)
3. Send success/failure notifications
4. Clean up local Parquet files (optional)

**Monitoring Tables**:
```sql
-- Job execution history
orchestrator_monitoring.jobs
  - job_id, source, job_type, status
  - created_at, completed_at, execution_time_seconds
  - rows_extracted, rows_inserted, rows_updated
  - error_message, log_file_path

-- Incremental state tracking
orchestrator_monitoring.extraction_state
  - source, table, site
  - last_extracted_at, last_cursor
  - last_job_id
```

---

## Data Storage Layers

### Layer 1: Local Temporary Storage

**Location**: `/tmp/orchestrator_{job_id}/`

**Purpose**: Temporary staging for type conversion and validation

**Files**:
```
/tmp/orchestrator_20250121T143022/
  ├── campaigns.parquet
  ├── adsets.parquet
  └── ads.parquet
```

**Retention**:
- Deleted after successful GCS upload (default)
- Preserved with `--keep-local` flag
- Preserved on failure for debugging

**Schema Enforcement**: Production table schema enforced during Parquet creation

### Layer 2: GCS Raw Archive

**Location**: `gs://{GCS_BUCKET}/{source}/raw/{resource}/{YYYY}/{MM}/{DD}/`

**Purpose**: Permanent historical archive of all extractions

**File Naming**: `{execution_id}_{timestamp}.parquet`

**Example**:
```
gs://my-datalake/facebook/raw/campaigns/
  ├── 2025/01/15/exec_001_20250115T120000.parquet
  ├── 2025/01/16/exec_002_20250116T120000.parquet
  ├── 2025/01/17/exec_003_20250117T120000.parquet
  └── 2025/01/21/exec_004_20250121T143022.parquet
```

**Retention**:
- Permanent (or GCS lifecycle policy)
- Recommended: 90-day lifecycle for cost optimization

**Access Pattern**:
```sql
-- Query historical data from raw files
CREATE EXTERNAL TABLE facebook_data.campaigns_historical
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://my-datalake/facebook/raw/campaigns/2025/01/*/*.parquet']
)
```

### Layer 3: GCS Staging

**Location**: `gs://{GCS_BUCKET}/{source}/staging/{resource}/latest/`

**Purpose**: Current version for BigQuery external tables

**File Naming**: `data.parquet` (always same name)

**Example**:
```
gs://my-datalake/facebook/staging/
  ├── campaigns/latest/data.parquet
  ├── adsets/latest/data.parquet
  └── posts/latest/data.parquet
```

**Retention**: Overwritten on each extraction run

**Access Pattern**: Via external tables in `{source}_staging` dataset

### Layer 4: BigQuery Staging

**Dataset**: `{source}_staging` (e.g., `facebook_staging`)

**Purpose**: External tables for staging data before production merge

**Tables**: `{table}_external` (e.g., `campaigns_external`)

**Schema**: Auto-detected from Parquet files

**Example**:
```sql
-- External table pointing to GCS staging
facebook_staging.campaigns_external
  - Points to: gs://bucket/facebook/staging/campaigns/latest/data.parquet
  - Schema: Auto-detected
  - Purpose: Staging for MERGE operation
```

### Layer 5: BigQuery Production

**Dataset**: `{source}_data` (e.g., `facebook_data`, `wordpress_data`)

**Purpose**: Production analytics tables

**Tables**: `{table}` (e.g., `campaigns`, `posts`)

**Schema**: Managed via YAML config with automatic evolution

**Features**:
- Hash-based deduplication
- Automatic schema evolution
- Incremental updates
- Historical tracking (row_hash, updated_at)

**Example**:
```sql
SELECT
  id,
  name,
  status,
  created_time,
  row_hash,        -- Hash of all columns for change detection
  updated_at,      -- Last update timestamp
  extracted_at     -- When data was extracted
FROM facebook_data.campaigns
WHERE updated_at >= '2025-01-01'
```

---

## Schema Management

### Schema Sources (Priority Order)

1. **YAML Config** (Primary): `configs/{source}.yaml`
2. **Production Table** (Fallback): Existing BigQuery table
3. **Runtime Discovery** (Last Resort): PyArrow + DuckDB analysis

### YAML Schema Definition

**File**: `configs/facebook.yaml`

```yaml
resources:
  campaigns:
    table_name: facebook_campaigns
    primary_key: [account_id, id]
    incremental:
      mode: lookback
      lookback_days: 7
    schema:
      id: STRING
      account_id: STRING
      name: STRING
      status: STRING
      created_time: TIMESTAMP
      updated_time: TIMESTAMP
      daily_budget: INT64
      lifetime_budget: INT64
      objective: STRING
```

### Schema Evolution Process

**When**: During MERGE operation in Stage 4

**Process**:
1. Compare source schema (external table) vs target schema (production table)
2. Identify new columns in source
3. For each new column:
   ```sql
   ALTER TABLE {production_table}
   ADD COLUMN IF NOT EXISTS {column_name} {column_type}
   ```
4. Update MERGE query to include new columns
5. Log schema changes to monitoring

**Example**:
```
Schema evolution: campaigns
  + Added column: bid_strategy (STRING)
  + Added column: adset_count (INT64)

MERGE query updated to include new columns
```

### Type Conversion Rules

**Parquet Creation** (Stage 1):

| BigQuery Type | Parquet Type | Conversion |
|---------------|--------------|------------|
| STRING | string | Direct |
| INT64 | int64 | pd.to_numeric() with coerce |
| FLOAT64 | double | pd.to_numeric() with coerce |
| BOOLEAN | bool | Map true/false/1/0 |
| TIMESTAMP | timestamp[us] | pd.to_datetime() |
| DATE | date32 | pd.to_datetime().dt.date |

**Mixed Type Handling**:
- Facebook action columns: Force to JSON strings
- Problematic patterns detected: `action`, `cost_per`, `conversion`
- Fallback: All complex types -> JSON strings

**BigQuery MERGE** (Stage 4):
```sql
-- Safe type casting in MERGE
SAFE_CAST(source.daily_budget AS INT64) AS daily_budget,
SAFE_CAST(source.created_time AS TIMESTAMP) AS created_time
```

### Rebuild Mode (Full Schema Discovery)

**Command**: `--rebuild` flag

**Process**:
1. Extract data from source
2. Use PyArrow + DuckDB to analyze 100% of data
3. Discover optimal types from actual values
4. Create new production table with discovered schema
5. Update YAML config with new schema

**Use Cases**:
- First-time table creation
- Schema correction after type issues
- Adding tables to existing source

**Example**:
```bash
# Rebuild campaigns table with full schema discovery
python orchestrate.py --source facebook --tables campaigns --rebuild
```

---

## Monitoring & Recovery

### Job Tracking

**Table**: `orchestrator_monitoring.jobs`

**Columns**:
```sql
job_id              STRING      -- Unique job identifier
source              STRING      -- facebook, wordpress
job_type            STRING      -- full, incremental
environment         STRING      -- prod, staging, dev
status              STRING      -- running, completed, failed
created_at          TIMESTAMP   -- Job start time
completed_at        TIMESTAMP   -- Job end time
execution_time_seconds FLOAT64  -- Duration
rows_extracted      INT64       -- Total rows extracted
rows_inserted       INT64       -- Rows inserted to production
rows_updated        INT64       -- Rows updated in production
error_message       STRING      -- Error details if failed
log_file_path       STRING      -- Path to log file
```

**Query Examples**:
```sql
-- Recent job history
SELECT
  job_id,
  source,
  status,
  created_at,
  execution_time_seconds,
  rows_extracted
FROM orchestrator_monitoring.jobs
WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY created_at DESC

-- Failed jobs
SELECT *
FROM orchestrator_monitoring.jobs
WHERE status = 'failed'
  AND created_at >= CURRENT_DATE()
ORDER BY created_at DESC
```

### Incremental State Tracking

**Table**: `orchestrator_monitoring.extraction_state`

**Purpose**: Store last extraction timestamp for incremental updates

**Columns**:
```sql
source           STRING      -- facebook, wordpress
table_name       STRING      -- campaigns, posts
site             STRING      -- Site/account identifier
last_extracted_at TIMESTAMP  -- Last successful extraction
last_cursor      STRING      -- API cursor for pagination
last_job_id      STRING      -- Job that updated this state
updated_at       TIMESTAMP   -- State update time
```

**Usage**:
```python
# Get last extraction time for incremental
last_run = get_last_extraction_time(source, table, site)

# Calculate lookback window
lookback_start = last_run - timedelta(days=7)  # 7-day overlap

# Extract only new/changed data
data = extract_incremental(since=lookback_start)
```

### Error Handling & Recovery

**Automatic Retry**:
- Rate limits: Exponential backoff (up to 60 min for error 80004)
- Network errors: 3 attempts with exponential backoff
- Transient GCS errors: 3 attempts with exponential backoff

**Recovery Checkpoints**:
```python
# Extraction checkpoints saved to /tmp/recovery_*
checkpoint_types:
  - EXTRACTION_STARTED
  - EXTRACTION_COMPLETE
  - GCS_UPLOAD_COMPLETE
  - EXTERNAL_TABLE_COMPLETE
  - TRANSFORM_COMPLETE
```

**Manual Recovery**:
```bash
# Retry failed job
python orchestrate.py --source facebook --tables campaigns --retry-job JOB_ID

# Resume from checkpoint
python orchestrate.py --source facebook --tables campaigns --resume

# Clean up failed job artifacts
python shared/recovery_cleanup.py --job-id JOB_ID
```

### Alerting

**Email Notifications**:
```yaml
# configs/alerting.yaml
email:
  enabled: true
  smtp_host: ${SMTP_HOST}
  smtp_port: 587
  recipients:
    - data-team@company.com

alerts:
  job_failure: true
  job_success: false  # Only alert on failures
  long_running_job: true  # Alert if job > 2 hours
```

**Alert Types**:
- Job failure with error details
- Long-running jobs (configurable threshold)
- Schema evolution changes
- High error rates

---

## Data Quality Checks

### Built-in Validations

**Extraction Stage**:
- Row count > 0 (warning if empty)
- Schema consistency across batches
- Type conversion success rate
- Duplicate primary key detection

**Transform Stage**:
- External table row count = extracted row count
- Production table row count increases or stays same
- No NULL values in primary key columns
- Schema compatibility (source vs target)

### Custom Validations

**Add to YAML config**:
```yaml
resources:
  campaigns:
    validations:
      - type: not_null
        columns: [id, account_id, name]
      - type: unique
        columns: [id, account_id]
      - type: range
        column: daily_budget
        min: 0
        max: 10000000
```

---

## Performance Optimization

### Extraction Performance

**Facebook**:
- Batch requests: 50 accounts per batch
- Parallel extraction: Up to 10 concurrent requests
- Date chunking: 89-day chunks for insights
- Field filtering: Only request needed fields

**WordPress**:
- SSH connection pooling
- Batch size: 10,000 rows per query
- Parallel sites: Process multiple sites concurrently

### BigQuery Performance

**Partitioning**:
```sql
-- Partition production tables by date
CREATE TABLE facebook_data.campaigns
PARTITION BY DATE(created_time)
AS SELECT * FROM ...
```

**Clustering**:
```sql
-- Cluster by common query columns
CREATE TABLE facebook_data.campaigns
PARTITION BY DATE(created_time)
CLUSTER BY account_id, status
AS SELECT * FROM ...
```

**MERGE Optimization**:
- Use primary key for ON condition (indexed)
- Use hash for change detection (faster than column comparison)
- Limit MERGE to changed rows only

---

## Cost Optimization

### GCS Storage Costs

**Lifecycle Policy**:
```bash
# Set 90-day lifecycle for raw archive
gsutil lifecycle set lifecycle.json gs://{bucket}
```

**lifecycle.json**:
```json
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {
          "age": 90,
          "matchesPrefix": ["facebook/raw/", "wordpress/raw/"]
        }
      }
    ]
  }
}
```

### BigQuery Costs

**Query Optimization**:
- Use partitioned tables
- Use clustering for common filters
- Avoid `SELECT *` - specify columns
- Use external tables for one-time analysis

**Incremental Updates**:
- Reduces processing volume (only new/changed data)
- 7-day lookback: ~7% of data vs full refresh

---

## Troubleshooting

### Common Issues

**Issue**: No data extracted (0 rows)
**Solution**:
- Check incremental lookback window (`--refresh full` to override)
- Verify API credentials
- Check date filters

**Issue**: Schema mismatch error
**Solution**:
- Use `--rebuild` to recreate table with correct schema
- Check YAML schema definition
- Review type conversion logs

**Issue**: Rate limit errors
**Solution**:
- Automatic retry with backoff (up to 60 min)
- Reduce batch size in YAML config
- Spread extractions across time

**Issue**: GCS upload failed
**Solution**:
- Verify `GCS_BUCKET` environment variable
- Check service account permissions
- Review GCS bucket location

---

## Best Practices

### Development
1. Test with `--extract-only` first (no GCS/BigQuery changes)
2. Use `--tables` to limit scope during testing
3. Use `--log-level debug` for detailed logging
4. Keep local files with `--keep-local` for debugging

### Production
1. Use incremental mode for daily updates
2. Schedule full refresh weekly/monthly
3. Monitor job execution times
4. Set up email alerts for failures
5. Review schema evolution logs

### Data Quality
1. Define YAML schemas for all tables
2. Test with `--rebuild` before production
3. Monitor row count changes
4. Review hash merge statistics (inserted/updated)
5. Validate data in BigQuery after extraction

---

## Appendix: Data Flow Example

**Scenario**: Extract Facebook campaigns (incremental)

```bash
# Command
python orchestrate.py \
  --source facebook \
  --tables campaigns \
  --refresh incremental \
  --log-level info
```

**Stage-by-Stage Flow**:

```
1. EXTRACTION (orchestrate.py -> facebook_extractor.py)
   ├─ Load YAML config: configs/facebook.yaml
   ├─ Get last extraction: 2025-01-14T00:00:00Z (from extraction_state)
   ├─ Calculate lookback: 2025-01-14 - 7 days = 2025-01-07
   ├─ API call: GET /act_123456/campaigns?since=2025-01-07
   ├─ Extract 1,234 campaigns
   ├─ Convert to Parquet: /tmp/orchestrator_exec001/campaigns.parquet
   └─ Output: {'total_rows': 1234, 'table_files': {'campaigns': '/tmp/.../campaigns.parquet'}}

2. GCS UPLOAD (orchestrate.py -> gcs_storage.py)
   ├─ Upload to RAW: gs://bucket/facebook/raw/campaigns/2025/01/21/exec_001_20250121T143022.parquet
   ├─ Upload to STAGING: gs://bucket/facebook/staging/campaigns/latest/data.parquet
   └─ Output: {'raw_gcs_path': '...', 'staging_gcs_path': '...'}

3. EXTERNAL TABLE (orchestrate.py -> external_tables.py)
   ├─ Create external table: facebook_staging.campaigns_external
   ├─ Point to: gs://bucket/facebook/staging/campaigns/latest/data.parquet
   ├─ Verify row count: 1,234 rows
   └─ Output: facebook_staging.campaigns_external

4. TRANSFORM (orchestrate.py -> bigquery_utils.py)
   ├─ Check production table exists: facebook_data.campaigns [PASS]
   ├─ Compare schemas: external vs production
   ├─ Schema evolution: No changes needed
   ├─ Execute MERGE:
   │  ├─ ON: target.id = source.id AND target.account_id = source.account_id
   │  ├─ WHEN NOT MATCHED: INSERT (234 new rows)
   │  ├─ WHEN MATCHED AND hash mismatch: UPDATE (89 changed rows)
   │  └─ WHEN MATCHED AND hash match: NO ACTION (911 unchanged)
   └─ Output: {'inserted': 234, 'updated': 89, 'deleted': 0}

5. MONITORING (orchestrate.py -> monitoring.py)
   ├─ Update job status: completed
   ├─ Update extraction_state: last_extracted_at = 2025-01-21T14:30:22Z
   ├─ Clean up local file: /tmp/orchestrator_exec001/campaigns.parquet
   └─ Send success notification (if configured)

Result:
  [PASS] 1,234 rows extracted from Facebook API
  [PASS] 1,234 rows uploaded to GCS (raw + staging)
  [PASS] 1,234 rows in external table
  [PASS] 234 rows inserted + 89 rows updated in production
  [PASS] Total production rows: 45,678
  [PASS] Execution time: 3m 42s
```

---

**End of Data Flow Manual**

For operational instructions, see [USER_MANUAL.md](USER_MANUAL.md)
For development guide, see [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md)
