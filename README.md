# Orchestrator - Universal Data Pipeline

**Proprietary Software** - Copyright © 2025 Bionews. All Rights Reserved.

Production-grade data extraction and loading system for Facebook, WordPress, and other data sources into Google BigQuery with comprehensive monitoring and automated data lake management.

> **NEW** (2025-10-26): Added **job chaining** (`--on-success`, `--on-failure`, `--on-finish`), machine tracking, and job lineage. See [JOB_CHAINING_GUIDE.md](JOB_CHAINING_GUIDE.md).

---

## Overview

**Orchestrator** is a YAML-driven data pipeline that extracts data from multiple sources and loads it into BigQuery for analytics. Built for reliability, scalability, and ease of use.

### Key Features

- **Multi-Source Support**: Facebook Graph API, WordPress MySQL, extensible plugin architecture
- **Data Lake Architecture**: Raw archive (GCS) -> Staging -> Production with automatic lifecycle management
- **Incremental Updates**: Smart lookback windows with hash-based deduplication
- **Schema Management**: YAML-driven schemas with automatic evolution
- **Production Ready**: Comprehensive monitoring, alerting, error recovery, and retry logic
- **Type Safety**: Robust Parquet conversion handles mixed types and complex data structures
- **CI/CD Testing**: Automated test suite for code quality validation
- **Testing & Development**: Schema prefix/suffix for test tables, runtime config overrides with `--vars`
- **Job Chaining**: Automated workflows with `--on-success`, `--on-failure`, `--on-finish`
- **Machine Tracking**: Cross-platform job monitoring with hostname, OS, PID, and remote killing capabilities
- **Job Lineage**: Track parent-child job relationships for workflow analysis

### Quick Start

```bash
# 1. Create and activate a virtual environment (Python 3.13)
python -m venv .venv
#    Windows (PowerShell):  .venv\Scripts\Activate.ps1
#    Windows (Git Bash):    source .venv/Scripts/activate
#    macOS/Linux:           source .venv/bin/activate

# 2. Install dependencies (includes pytest)
pip install -r requirements.txt

# 3. Configure credentials
cp env.example .env
# Edit .env with your credentials

# 4. Extract Facebook campaigns
python orchestrate.py --source facebook --env prod --tables campaigns

# 5. Extract WordPress posts
python orchestrate.py --source wordpress --env prod --tables posts
```

### Running the tests

```bash
# Full CI suite (lint + checks + unit tests)
python run_tests.py

# Unit tests directly
python -m pytest tests/unit -q
```

The full suite must run with **all** of `requirements.txt` installed -- in
particular `fabric`/`paramiko`, the SSH stack the WordPress and LimeSurvey
extractors depend on. If `tests/unit/test_wordpress_circuit_breaker.py` errors at
collection with `ModuleNotFoundError: No module named 'fabric'`, the environment
is incomplete: re-run `pip install -r requirements.txt` inside the active venv.
Do not skip that test to work around a missing dependency -- fabric is a core ETL
dependency, not an optional one.

> **Note on the virtual environment.** Create the venv yourself as shown above
> (the directory is git-ignored and is **not** included in a checkout). Any name
> works -- `.venv` is the documented default; an existing `venv_test/` in a
> working copy serves the same purpose, but only if `pip install -r
> requirements.txt` has been run in it (an under-provisioned venv missing fabric
> is the usual cause of import errors). There is no committed/shared interpreter,
> so "use the project's Python" means "activate your own venv after installing
> the full requirements." All tooling (`orchestrate.py`, `pytest`,
> `run_tests.py`) runs on Python 3.13 from that environment.

---

## Architecture

```
┌────────────────┐
│  Data Sources  │  Facebook API, WordPress MySQL
└────────┬───────┘
         │
         ▼
┌────────────────┐
│   Extraction   │  Python plugins with retry logic
└────────┬───────┘
         │
         ▼
┌────────────────┐
│   GCS Archive  │  Date-partitioned Parquet files
│  (Data Lake)   │  gs://{bucket}/{source}/raw/{YYYY}/{MM}/{DD}/
└────────┬───────┘
         │
         ▼
┌────────────────┐
│   BigQuery     │  External tables -> Staging -> Production
│   Analytics    │  Hash-based MERGE with deduplication
└────────────────┘
```

### Data Flow

1. **Extract**: Pull data from source API/database
2. **Transform**: Convert to Parquet with schema enforcement
3. **Upload**: Save to GCS (raw archive + staging)
4. **Stage**: Create BigQuery external tables
5. **Merge**: Hash-based merge to production tables
6. **Monitor**: Track metrics and send alerts

---

## Supported Data Sources

### Facebook (18 Tables)

**Core Tables**:
- Campaigns, AdSets, Ads, Ad Creatives
- Pages, Posts

**Insights Tables** (ASYNC extraction with 90-day batching):
- Campaign Insights, Campaign Insights Actions
- AdSet Insights, AdSet Insights Actions
- Ad Insights, Ad Insights Actions
- Page Insights, Post Insights

**Features**:
- Automatic rate limit handling (up to 1 hour backoff for error 80004)
- Batch API requests for efficiency
- 90-day chunking for insights (Facebook 93-day limit)
- Mixed type handling for action/metric columns (100+ action types)
- Page-specific access tokens for page/post insights
- Multi-tenant support (account_id isolation)

**Example**:
```bash
# Extract core tables (specific site)
python orchestrate.py \
  --source facebook \
  --env prod \
  --tables campaigns adsets ads \
  --sites 101943943298146

# Extract insights (ASYNC with date range)
python orchestrate.py \
  --source facebook \
  --env prod \
  --tables campaign_insights \
  --sites 101943943298146 \
  --start-date 2025-09-01 \
  --end-date 2025-10-23

# Extract all core tables for all sites
python orchestrate.py \
  --source facebook \
  --env prod \
  --group core
```

### WordPress

**Tables**:
- Posts, PostMeta, Users, Comments

**Features**:
- SSH tunnel to MySQL
- Multi-site extraction
- Incremental updates via post_modified
- Site-specific field injection

**Example**:
```bash
# Extract specific tables
python orchestrate.py \
  --source wordpress \
  --env prod \
  --tables posts postmeta

# Extract core group
python orchestrate.py \
  --source wordpress \
  --env prod \
  --group core
```

---

## Installation

### Prerequisites

- Python 3.9+
- Google Cloud credentials (service account with BigQuery/GCS access)
- GCS bucket
- BigQuery datasets (staging + production per source)
- Facebook API access token
- WordPress site access (SSH for MySQL)

### Setup

```bash
# 1. Clone repository (internal)
git clone https://github.com/bmacinnis/orchestrator.git
cd orchestrator

# 2. Create and activate a virtual environment (Python 3.13), then install deps.
#    The venv directory is git-ignored and not part of a checkout -- create it
#    locally. .venv is the documented default (any name works).
python -m venv .venv
source .venv/Scripts/activate   # Windows Git Bash; see Quick Start for other shells
pip install -r requirements.txt

# 3. Configure environment
cp env.example .env
vim .env  # Add your credentials

# 4. Verify credentials
export GOOGLE_APPLICATION_CREDENTIALS=c:\gcp\service-account-bionews-pipeline.json

# 5. Create BigQuery datasets (if not exists)
bq mk bi-data-391216:facebook_staging
bq mk bi-data-391216:facebook_data
bq mk bi-data-391216:wordpress_staging
bq mk bi-data-391216:wordpress_data
bq mk bi-data-391216:orchestrator_monitoring_prod

# 6. Create GCS bucket (if not exists)
gsutil mb -l US gs://orchestrator-data-lake
```

### Environment Variables

**Required** (`.env` file):
```bash
# Google Cloud
GCP_PROJECT_ID=bi-data-391216
GOOGLE_APPLICATION_CREDENTIALS=c:\gcp\service-account-bionews-pipeline.json
GCS_BUCKET=orchestrator-data-lake

# Facebook
FB_ACCESS_TOKEN=your_long_lived_token
FB_APP_ID=your_app_id
FB_APP_SECRET=your_app_secret

# WordPress
WP_SITES_API_URL=https://pipeline.bionews.com/query/sites
WP_API_KEY=your_api_key
WP_SSH_KEY_PATH=c:\Users\bmaci\.ssh\id_rsa

# Email notifications (optional)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
```

---

## Usage

### Basic Commands

**Important**: All commands now require `--env` parameter:

```bash
# Facebook incremental (default)
python orchestrate.py --source facebook --env prod --tables campaigns

# WordPress incremental
python orchestrate.py --source wordpress --env prod --tables posts

# Full refresh
python orchestrate.py --source facebook --env prod --tables campaigns --refresh full

# Rebuild table (drops and recreates)
python orchestrate.py --source facebook --env prod --tables campaigns --rebuild

# Multiple tables
python orchestrate.py --source facebook --env prod --tables campaigns adsets ads

# Specific sites (plain numeric ID, no act_ prefix needed)
python orchestrate.py --source facebook --env prod --tables campaigns --sites 101943943298146

# Table group extraction
python orchestrate.py --source facebook --env prod --group core

# Date range (for insights)
python orchestrate.py \
  --source facebook \
  --env prod \
  --tables campaign_insights \
  --start-date 2025-09-01 \
  --end-date 2025-10-23
```

### Testing & Validation

```bash
# Run CI/CD test suite (fast, ~2 minutes)
python run_tests.py

# Run specific test category
python run_tests.py --category syntax      # Syntax validation
python run_tests.py --category config      # Configuration tests
python run_tests.py --category quality     # Data quality tests

# Validate configuration only (no extraction)
python orchestrate.py --source facebook --env prod --validate-only

# Dry run (shows what would be extracted)
python orchestrate.py --source facebook --env prod --tables campaigns --dry-run

# List available tables
python orchestrate.py --source facebook --env prod --list-tables

# List available sites
python orchestrate.py --source facebook --env prod --list-sites

# Extract only (no GCS/BigQuery changes)
python orchestrate.py --source facebook --env prod --tables campaigns --extract-only

# Verbose logging
python orchestrate.py --source facebook --env prod --tables campaigns --log-level verbose
```

### Workflow Automation & Job Chaining

**Simple workflow** (extract pages → posts on success):
```bash
python orchestrate.py --source facebook --env prod --tables pages \
  --on-success "python orchestrate.py --source facebook --env prod --tables posts"
```

**Multi-step workflow** (extract → transform → load):
```bash
python orchestrate.py --source facebook --env prod --tables campaigns \
  --on-success "python transform_campaigns.py" \
  --on-finish "python send_report.py"
```

**Error handling workflow**:
```bash
python orchestrate.py --source wordpress --env prod --tables posts \
  --on-success "python scripts/validate_data.py" \
  --on-failure "python scripts/send_alert.py" \
  --on-finish "python scripts/cleanup_temp.py"
```

**Job lineage tracking**:
```bash
# Parent job passes its ID to child
python orchestrate.py --source facebook --env prod --tables pages \
  --pass-job-id \
  --on-success "python orchestrate.py --source facebook --env prod --tables posts"

# Query lineage in BigQuery
SELECT j1.job_id as parent, j1.tables as parent_tables,
       j2.job_id as child, j2.tables as child_tables
FROM `orchestrator_monitoring.jobs` j1
JOIN `orchestrator_monitoring.jobs` j2 ON j1.job_id = j2.parent_job_id
WHERE j1.created_at >= CURRENT_DATE()
```

See [JOB_CHAINING_GUIDE.md](JOB_CHAINING_GUIDE.md) for complete documentation.

### Production Scheduling

**Crontab example** (Linux/Mac):
```cron
# Daily Facebook core tables (6 AM)
0 6 * * * cd /path/to/orchestrator && python orchestrate.py --source facebook --env prod --group core >> /var/log/facebook_daily.log 2>&1

# Daily Facebook insights (7 AM) - 7 day lookback
0 7 * * * cd /path/to/orchestrator && python orchestrate.py --source facebook --env prod --group insights >> /var/log/facebook_insights.log 2>&1

# Daily WordPress update (8 AM)
0 8 * * * cd /path/to/orchestrator && python orchestrate.py --source wordpress --env prod --group core >> /var/log/wordpress_daily.log 2>&1

# Weekly full refresh (Sunday 2 AM)
0 2 * * 0 cd /path/to/orchestrator && python orchestrate.py --source facebook --env prod --group core --refresh full >> /var/log/facebook_weekly.log 2>&1
```

**Windows Task Scheduler**:
```powershell
# Create scheduled task for daily Facebook extraction
schtasks /create /tn "Facebook Daily Extract" /tr "python c:\orchestrator\orchestrate.py --source facebook --env prod --group core" /sc daily /st 06:00
```

---

## Configuration

### YAML Configuration Files

**Structure**:
```
configs/
├── facebook.yaml       # Facebook: 18 tables with descriptions
├── wordpress.yaml      # WordPress: 4 tables
├── defaults.yaml       # Default settings
└── alerting.yaml       # Email/Slack alerts
```

**Key Configuration Features**:
- **Table Descriptions**: All 18 Facebook tables have descriptions
- **Primary Keys**: Multi-column PKs for deduplication
- **Incremental Mode**: Lookback windows per table
- **Field Filtering**: Exclude unwanted fields
- **Groups**: Table groups for batch extraction (core, insights, pages)

**Example** (`configs/facebook.yaml`):
```yaml
source:
  type: facebook
  api_version: v21.0

pipeline:
  staging_dataset: facebook_staging
  production_dataset: facebook_data

resources:
  campaigns:
    description: "Campaign-level data - optimization, budget, status"
    table_name: facebook_campaigns
    primary_key: [account_id, id]
    active: true

    incremental:
      mode: lookback
      lookback_days: 7

    schema:
      account_id: STRING
      id: STRING
      name: STRING
      status: STRING
      objective: STRING
      daily_budget: STRING
      lifetime_budget: STRING
      created_time: TIMESTAMP
      updated_time: TIMESTAMP
```

See [YAML_CONFIGURATION_MANUAL.md](docs/YAML_CONFIGURATION_MANUAL.md) for complete reference.

---

## Monitoring

### CI/CD Testing

```bash
# Run all CI/CD tests
python run_tests.py

# Output shows:
# - Syntax & import tests (10 tests)
# - Environment tests (4 tests)
# - Configuration validation (6 tests)
# - Data quality tests (4 tests)
# - Error handling tests (4 tests)

# Expected: 24-26 tests pass, 1-2 skip
```

### Job History (BigQuery)

```sql
-- Recent jobs
SELECT
  execution_id,
  source,
  table_name,
  status,
  start_time,
  end_time,
  TIMESTAMP_DIFF(end_time, start_time, SECOND) as duration_seconds,
  rows_extracted,
  rows_loaded
FROM `bi-data-391216.orchestrator_monitoring_prod.pipeline_executions`
WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY start_time DESC
LIMIT 50
```

### Failed Jobs

```sql
-- Jobs that failed today
SELECT
  execution_id,
  source,
  table_name,
  start_time,
  error_message
FROM `bi-data-391216.orchestrator_monitoring_prod.pipeline_executions`
WHERE status = 'failed'
  AND DATE(start_time) = CURRENT_DATE()
ORDER BY start_time DESC
```

---

## Data Lake Structure

### GCS Bucket Layout

```
gs://orchestrator-data-lake/
├── facebook/
│   ├── campaigns/
│   │   └── 2025/10/23/
│   │       └── exec_job_20251023_120145-abc123.parquet
│   ├── campaign_insights/
│   │   └── 2025/10/23/
│   │       └── exec_job_20251023_130022-def456.parquet
│   └── ...
│
└── wordpress/
    ├── posts/
    │   └── 2025/10/23/
    │       └── exec_job_20251023_080000-xyz789.parquet
    └── ...
```

**Features**:
- Date-partitioned by extraction date
- Execution ID in filename for traceability
- Permanent storage (or lifecycle policy)
- Complete historical record

### BigQuery Datasets

```
bi-data-391216
├── facebook_staging         # External tables pointing to GCS
│   ├── facebook_campaigns
│   ├── facebook_campaign_insights
│   └── ...
│
├── facebook_data           # Production tables (merged/deduplicated)
│   ├── facebook_campaigns
│   ├── facebook_adsets
│   ├── facebook_ads
│   ├── facebook_campaign_insights
│   ├── facebook_pages
│   ├── facebook_posts
│   └── ... (18 tables total)
│
├── wordpress_staging       # External tables
│   ├── wordpress_posts
│   └── ...
│
├── wordpress_data          # Production tables
│   ├── wordpress_posts
│   ├── wordpress_postmeta
│   └── ...
│
└── orchestrator_monitoring_prod  # Job tracking
    ├── pipeline_executions
    ├── job_metadata
    └── extraction_state
```

---

## Features

### Facebook-Specific Features

1. **ASYNC Insights Extraction**:
   - Campaign, AdSet, Ad insights use async API
   - Poll for completion (up to 2 hours)
   - Automatic retry on timeout

2. **90-Day Batching**:
   - Facebook API limit: 93 days per request
   - Automatic date chunking for longer ranges
   - Prevents API errors

3. **Rate Limit Handling**:
   - Error 80004: Sleep 1 hour then retry
   - Error 4: Exponential backoff (5min -> 60min)
   - Other errors: 3 retries with exponential backoff

4. **Page-Specific Tokens**:
   - Pages table provides page_access_token
   - Used for page_insights and post_insights
   - Automatic token management

5. **Action Field Handling**:
   - 100+ possible action types (sparse data)
   - Most fields NULL by design (campaign-specific)
   - Pre-conversion to strings for PyArrow compatibility

### Incremental Updates

**Default mode**: Extract only new/changed data

**How it works**:
1. Check last extraction time from monitoring
2. Calculate lookback window (default: 7 days)
3. Extract data since `last_run - lookback_days`
4. Merge to production using hash-based deduplication

**Benefits**:
- 10x faster than full refresh
- Reduces API calls
- Lower BigQuery costs

### Schema Evolution

**Automatic column addition**:
- New columns in source -> automatically added to production
- Schema evolution logged to monitoring
- Safe casting for type mismatches

**Example scenario**:
```
Day 1: Table has [id, name, status]
Day 2: API returns [id, name, status, budget]

Result: Column 'budget' automatically added as NULLABLE
```

### Hash-Based Deduplication

**Change detection**:
- SHA256 hash of all columns
- Compare hash instead of column-by-column comparison
- Update only changed rows

**MERGE logic**:
```sql
MERGE target USING source
ON target.account_id = source.account_id
   AND target.id = source.id
WHEN NOT MATCHED THEN INSERT          -- New rows
WHEN MATCHED AND _hash != _hash THEN UPDATE  -- Changed rows
WHEN MATCHED THEN DO NOTHING          -- Unchanged rows
```

---

## Command Reference

### Required Arguments

| Argument | Description | Example |
|----------|-------------|---------|
| `--source` | Data source | `facebook`, `wordpress` |
| `--env` | Environment | `prod`, `dev`, `test` |
| `--tables` or `--group` | Tables to extract | `campaigns` or `core` |

### Optional Arguments

| Argument | Description | Default | Example |
|----------|-------------|---------|---------|
| `--sites` | Sites/accounts (plain ID) | All from YAML | `101943943298146` |
| `--refresh` | Mode | `incremental` | `full` |
| `--rebuild` | Drop/recreate table | `False` | `--rebuild` |
| `--start-date` | Start date | Last extraction | `2025-09-01` |
| `--end-date` | End date | Today | `2025-10-23` |
| `--schema-prefix` | Prefix for table names | None | `test` (creates `test_campaigns`) |
| `--schema-suffix` | Suffix for table names | None | `_v2` (creates `campaigns_v2`) |
| `--vars` | Override config values | None | `max_rows=100` or `optimization.workers=10` |
| `--validate-only` | Validate config only | `False` | `--validate-only` |
| `--dry-run` | Show what would run | `False` | `--dry-run` |
| `--extract-only` | Local only | `False` | `--extract-only` |
| `--log-level` | Logging | `normal` | `minimal`, `verbose` |
| `--on-success` | Command on success | None | `"python next_step.py"` |
| `--on-failure` | Command on failure | None | `"python alert.py"` |
| `--on-finish` | Command always | None | `"python cleanup.py"` |
| `--pass-job-id` | Pass job ID to chain | `False` | `--pass-job-id` |
| `--pass-execution-id` | Pass exec ID to chain | `False` | `--pass-execution-id` |

**Full syntax**:
```bash
python orchestrate.py \
  --source {facebook|wordpress} \
  --env {prod|dev|test} \
  {--tables TABLE1 [TABLE2 ...] | --group GROUP_NAME} \
  [--sites SITE1 [SITE2 ...]] \
  [--refresh {incremental|full}] \
  [--rebuild] \
  [--start-date YYYY-MM-DD] \
  [--end-date YYYY-MM-DD] \
  [--schema-prefix PREFIX] \
  [--schema-suffix SUFFIX] \
  [--vars key=value] \
  [--validate-only] \
  [--dry-run] \
  [--extract-only] \
  [--log-level {minimal|normal|verbose}]
```

**New Testing Features**:
```bash
# Create test tables without modifying YAML
python orchestrate.py --source facebook --env prod \
  --tables campaigns \
  --schema-prefix test

# Override config at runtime
python orchestrate.py --source facebook --env prod \
  --tables campaign_insights \
  --vars optimization.max_parallel_workers=20 \
  --vars pipeline.batch_size=1000
```

---

## Troubleshooting

### Common Issues

**No data extracted (0 rows)**:
```bash
# Solution: Use full refresh
python orchestrate.py --source facebook --env prod --tables campaigns --refresh full
```

**Schema mismatch error**:
```bash
# Solution: Rebuild table with correct schema
python orchestrate.py --source facebook --env prod --tables campaigns --rebuild
```

**Rate limit errors (Facebook error 80004)**:
- Automatic retry after 1 hour
- Let it run, no action needed
- Check logs for "Sleeping for 3600 seconds"

**Mixed type errors in insights**:
- Fixed: Schema discovery pre-converts to strings
- Action fields handle mixed types ('1' vs 1)

**GCS upload failed**:
```bash
# Check environment variable
echo $GCS_BUCKET

# Test GCS access
gsutil ls gs://orchestrator-data-lake
```

**BigQuery permission denied**:
```bash
# Verify service account permissions
gcloud projects get-iam-policy bi-data-391216 \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:*"
```

---

## Documentation

### Complete Documentation Set

**User Guides**:
- [USER_MANUAL.md](docs/USER_MANUAL.md) - Detailed usage guide with examples
- [JOB_CHAINING_GUIDE.md](JOB_CHAINING_GUIDE.md) - Workflow automation & job chaining
- [PIPELINE_MANAGER_MANUAL.md](docs/PIPELINE_MANAGER_MANUAL.md) - Job monitoring & management CLI

**Technical Documentation**:
- [DATA_FLOW_MANUAL.md](docs/DATA_FLOW_MANUAL.md) - Complete architecture and data flow
- [IMPLEMENTATION_GUIDE.md](docs/IMPLEMENTATION_GUIDE.md) - Developer documentation
- [YAML_CONFIGURATION_MANUAL.md](docs/YAML_CONFIGURATION_MANUAL.md) - YAML configuration reference
- [MODULE_MANUAL.md](docs/MODULE_MANUAL.md) - Python module documentation (43 modules)

**Process & Compliance**:
- [CI_CD_GUIDE.md](docs/CI_CD_GUIDE.md) - CI/CD testing guide
- [GITHUB_COMPLIANCE.md](docs/GITHUB_COMPLIANCE.md) - GitHub best practices
- [LINUX_DEPLOYMENT_GUIDE.md](docs/LINUX_DEPLOYMENT_GUIDE.md) - Linux deployment
- [SECURITY.md](docs/SECURITY.md) - Security policy

**Analysis & Reference**:
- [DEPENDENCY_ANALYSIS.md](DEPENDENCY_ANALYSIS.md) - Job dependency feature analysis
- [FILES_TO_EXCLUDE.md](docs/FILES_TO_EXCLUDE.md) - Git exclusion guide

### Quick Reference

- **Getting Started**: See Quick Start above
- **Configuration**: See [YAML_CONFIGURATION_MANUAL.md](docs/YAML_CONFIGURATION_MANUAL.md)
- **Testing**: Run `python run_tests.py`
- **Troubleshooting**: See section above
- **Security**: See [SECURITY.md](SECURITY.md)

---

## Project Structure

```
orchestrator/
├── orchestrate.py              # Main entry point (62KB)
├── pipeline_manager.py         # CLI management tool (33KB)
├── run_tests.py               # CI/CD test suite (30KB)
├── requirements.txt            # Python dependencies
├── .env                        # Environment variables (git-ignored)
├── .gitignore                  # Comprehensive exclusions
├── LICENSE                     # Proprietary license
├── SECURITY.md                 # Security policy
├──── configs/                    # YAML configurations
│   ├── facebook.yaml          # 18 tables, all described
│   ├── wordpress.yaml         # 4 tables
│   ├── defaults.yaml          # Global defaults
│   └── alerting.yaml          # Notification config
├── docs/                       # Complete documentation (7 files)
│   ├── DATA_FLOW_MANUAL.md
│   ├── USER_MANUAL.md
│   ├── IMPLEMENTATION_GUIDE.md
│   ├── YAML_CONFIGURATION_MANUAL.md
│   ├── MODULE_MANUAL.md
│   ├── CI_CD_GUIDE.md
│   └── GITHUB_COMPLIANCE.md
├── plugins/                    # Source extractors
│   ├── facebook_extractor.py  # 1800+ lines
│   ├── wordpress_extractor.py
│   └── generic_extractor.py
├── shared/                     # 36 utility modules
│   ├── bigquery_utils.py
│   ├── gcs_storage.py
│   ├── config_loader.py
│   ├── monitoring.py
│   ├── transform.py
│   ├── schema_discovery.py
│   └── ...
└── .github/
    └── workflows/
        └── ci.yml             # GitHub Actions CI/CD
```

---

## CI/CD & Testing

### Automated Testing

**Run after every commit**:
```bash
python run_tests.py
```

**Test Categories**:
1. **Syntax & Imports** (10 tests) - Compile all Python modules
2. **Environment** (4 tests) - Verify credentials and access
3. **Configuration** (6 tests) - Validate YAML configs
4. **Data Quality** (4 tests) - Check for duplicates, descriptions
5. **Error Handling** (4 tests) - Graceful error handling

**Expected Results**:
- [OK] 24-26 tests pass
- [WARNING] 1-2 tests skip (optional tests)
- [FAIL] 0 tests fail (all must pass)

### GitHub Actions

Automated CI/CD runs on every push/PR:
- Syntax validation
- Configuration tests
- Linting (flake8, pylint)
- Test results uploaded as artifacts

---

## Performance

### Extraction Speed

**Facebook**:
- Core tables: ~1000 rows/min
- Insights (ASYNC): ~500 rows/min (limited by API)
- Pages: ~100 pages/min
- Posts: ~200 posts/min

**WordPress**:
- Posts: ~10,000 rows/min
- Limited by SSH tunnel latency

### Optimization Tips

1. **Use incremental mode** for daily updates (10x faster)
2. **Use table groups** (`--group core`) for batch extraction
3. **Schedule insights off-peak** (ASYNC takes 30-120 min)
4. **Use site filtering** for testing (`--sites 101943943298146`)
5. **Run CI tests locally** before pushing (`python run_tests.py`)

---

## Security

### Best Practices

1. **Credentials**:
   - Never commit `.env` file (git-ignored)
   - Use GCP Secret Manager in production
   - Rotate credentials quarterly

2. **Access Control**:
   - Service accounts with minimal permissions
   - Separate credentials per environment
   - Enable audit logging

3. **Data Protection**:
   - TLS for all API connections
   - GCS/BigQuery encryption at rest (default)
   - PII handling per compliance requirements

See [SECURITY.md](SECURITY.md) for complete security policy.

---

## License

**Proprietary** - Copyright © 2025 Bionews. All Rights Reserved.

This software is proprietary and confidential. Unauthorized copying, modification, distribution, or use is strictly prohibited. For authorized use only by Bionews personnel.

See [LICENSE](LICENSE) for full terms.

---

## Support

### Internal Support

- **Data Team**: data-team@bionews.com
- **Security Issues**: security@bionews.com
- **GitHub Issues**: https://github.com/bmacinnis/orchestrator/issues

### Getting Help

When reporting issues, include:
1. Command run
2. Error message (full traceback)
3. Log output (use `--log-level verbose`)
4. Expected vs actual behavior
5. Environment (Windows/Linux, Python version)

---

## Quick Links

- [Data Flow Manual](docs/DATA_FLOW_MANUAL.md) - Architecture & design
- [User Manual](docs/USER_MANUAL.md) - Detailed usage guide
- [Implementation Guide](docs/IMPLEMENTATION_GUIDE.md) - Developer docs
- [YAML Configuration Manual](docs/YAML_CONFIGURATION_MANUAL.md) - Config reference
- [Module Manual](docs/MODULE_MANUAL.md) - Python code reference (43 modules)
- [CI/CD Guide](docs/CI_CD_GUIDE.md) - Testing & validation
- [GitHub Compliance](docs/GITHUB_COMPLIANCE.md) - Repository best practices

---

## Version

**Current Version**: 1.1.0
**Last Updated**: 2025-10-26
**Python**: 3.9+
**Facebook API**: v21.0

**Recent Updates**:
- Job chaining with `--on-success`, `--on-failure`, `--on-finish`
- Machine tracking (hostname, OS, PID) for cross-platform monitoring
- Job lineage tracking with `parent_job_id`
- Fixed machine tracking on Linux with parameterized SQL queries

---

**Built with**: Python 3.9+, Google Cloud Platform, Facebook Graph API v21.0, WordPress MySQL, BigQuery, GCS, Parquet, PyArrow, DuckDB
