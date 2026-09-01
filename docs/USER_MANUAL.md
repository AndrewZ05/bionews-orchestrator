# User Manual
## Running Data Extraction Jobs

**Last Updated**: 2025-10-26
**Version**: 1.1

---

## Table of Contents
1. [Quick Start](#quick-start)
2. [Installation & Setup](#installation--setup)
3. [Running Facebook Extractions](#running-facebook-extractions)
4. [Running WordPress Extractions](#running-wordpress-extractions)
5. [Command Reference](#command-reference)
6. [Monitoring Jobs](#monitoring-jobs)
7. [Troubleshooting](#troubleshooting)
8. [Best Practices](#best-practices)

---

## Quick Start

### Prerequisites
- Python 3.8+
- Google Cloud credentials configured
- Facebook/WordPress API credentials
- GCS bucket created

### Basic Commands

```bash
# Facebook: Extract campaigns (incremental)
python orchestrate.py --source facebook --tables campaigns

# WordPress: Extract posts from all sites (incremental)
python orchestrate.py --source wordpress --tables posts

# Full refresh with rebuild (recreate table)
python orchestrate.py --source facebook --tables campaigns --refresh full --rebuild
```

---

## Installation & Setup

### 1. Clone Repository

```bash
git clone <repository-url>
cd orchestrator
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

**Key packages**:
- `facebook-business` (Facebook Graph API)
- `google-cloud-bigquery` (BigQuery)
- `google-cloud-storage` (GCS)
- `pandas`, `pyarrow` (Data processing)
- `sqlalchemy`, `pymysql` (WordPress MySQL)
- `sshtunnel` (WordPress SSH tunnels)

### 3. Configure Environment Variables

**Copy example file**:
```bash
cp env.example .env
```

**Edit `.env` file**:
```bash
# Facebook credentials
FACEBOOK_ACCESS_TOKEN=EAAxxxxxxxxxxxxx
FACEBOOK_APP_ID=123456789
FACEBOOK_APP_SECRET=abcdef123456

# WordPress credentials
WP_SITES_API_URL=https://pipeline.bionews.com/query/sites
WP_API_KEY=your_api_key_here

# Google Cloud
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
GCS_BUCKET=my-data-lake-bucket

# Email notifications (optional)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
```

### 4. Verify Google Cloud Authentication

```bash
# Set credentials
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Test connection
python -c "from google.cloud import bigquery; client = bigquery.Client(); print(f'Project: {client.project}')"
```

**Expected output**:
```
Project: bi-data-391216
```

### 5. Create GCS Bucket (if needed)

```bash
# Create bucket
gsutil mb -p bi-data-391216 -l US gs://my-data-lake-bucket

# Verify access
gsutil ls gs://my-data-lake-bucket
```

### 6. Verify Configuration Files

```bash
# List available configurations
ls configs/

# Should see:
# - facebook.yaml
# - wordpress.yaml
# - defaults.yaml
# - alerting.yaml
```

---

## Running Facebook Extractions

### Understanding Facebook Entities

Facebook has two main entity types:

**Ad Accounts** (format: `act_123456`)
- Campaigns
- AdSets
- Ads
- Ad Insights
- Campaign Insights
- AdSet Insights

**Pages** (format: `123456789` - numeric ID)
- Posts
- Page Insights
- Post Insights

### Basic Facebook Commands

#### Extract Campaigns (Incremental)

```bash
python orchestrate.py \
  --source facebook \
  --tables campaigns \
  --sites act_123456
```

**What it does**:
- Extracts campaigns from ad account `act_123456`
- Uses incremental mode (last 7 days + new data)
- Uploads to GCS
- Merges to `facebook_data.campaigns`

#### Extract Multiple Tables

```bash
python orchestrate.py \
  --source facebook \
  --tables campaigns adsets ads \
  --sites act_123456
```

#### Extract from Multiple Ad Accounts

```bash
python orchestrate.py \
  --source facebook \
  --tables campaigns \
  --sites act_123456 act_789012
```

#### Extract Page Posts

```bash
python orchestrate.py \
  --source facebook \
  --tables posts \
  --sites 123456789
```

**Note**: Page ID must be numeric (not `act_` prefix)

#### Extract Insights with Date Range

```bash
python orchestrate.py \
  --source facebook \
  --tables campaign_insights \
  --sites act_123456 \
  --start-date 2025-01-01 \
  --end-date 2025-01-21
```

### Facebook Extraction Modes

#### Incremental (Default)

```bash
python orchestrate.py --source facebook --tables campaigns
```

**Behavior**:
- Looks back 7 days from last extraction
- Extracts only new/changed records
- Merges to production (INSERT/UPDATE)
- Fast, efficient for daily updates

#### Full Refresh

```bash
python orchestrate.py --source facebook --tables campaigns --refresh full
```

**Behavior**:
- Extracts ALL campaigns (no date filter)
- Merges to production
- Slower but ensures completeness
- Use weekly/monthly

#### Rebuild Mode

```bash
python orchestrate.py --source facebook --tables campaigns --refresh full --rebuild
```

**Behavior**:
- Extracts all data
- Analyzes 100% of data to discover optimal schema
- **DROPS and RECREATES** production table
- Updates YAML config with new schema
- Use for schema fixes or new tables

**WARNING**: `--rebuild` deletes existing production table!

### Auto-Discovery Mode

```bash
python orchestrate.py \
  --source facebook \
  --tables campaigns \
  --sites all
```

**What it does**:
- Discovers all ad accounts from Facebook API
- Extracts campaigns from each account
- Discovers pages associated with accounts
- Useful for initial setup

### Facebook Examples

#### Example 1: Daily Campaign Update

```bash
# Run daily via cron
python orchestrate.py \
  --source facebook \
  --tables campaigns adsets ads \
  --sites act_123456 \
  --log-level info
```

**Crontab entry**:
```cron
# Run at 6 AM daily
0 6 * * * cd /path/to/orchestrator && python orchestrate.py --source facebook --tables campaigns adsets ads --sites act_123456 >> /var/log/facebook_daily.log 2>&1
```

#### Example 2: Weekly Full Refresh

```bash
# Run weekly via cron
python orchestrate.py \
  --source facebook \
  --tables campaigns adsets ads campaign_insights \
  --sites act_123456 \
  --refresh full
```

**Crontab entry**:
```cron
# Run Sunday at 2 AM
0 2 * * 0 cd /path/to/orchestrator && python orchestrate.py --source facebook --tables campaigns adsets ads campaign_insights --sites act_123456 --refresh full >> /var/log/facebook_weekly.log 2>&1
```

#### Example 3: Historical Insights Backfill

```bash
# Extract insights for specific date range
python orchestrate.py \
  --source facebook \
  --tables campaign_insights \
  --sites act_123456 \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --refresh full
```

#### Example 4: Multi-Account Extraction

```bash
# Extract from multiple accounts in parallel
python orchestrate.py \
  --source facebook \
  --tables campaigns adsets ads \
  --sites act_111111 act_222222 act_333333
```

#### Example 5: Test Extraction (No BigQuery Changes)

```bash
# Extract to local files only (no GCS, no BigQuery)
python orchestrate.py \
  --source facebook \
  --tables campaigns \
  --sites act_123456 \
  --extract-only \
  --keep-local
```

**Output**: Files in `/tmp/orchestrator_{job_id}/campaigns.parquet`

### Facebook Site Filtering

Filter extraction to specific sites/pages:

```bash
# Extract only specific sites (not all from YAML)
python orchestrate.py \
  --source facebook \
  --tables posts \
  --sites 123456789 987654321 \
  --site-filter
```

**Use case**: Extract from subset of configured sites

---

## Running WordPress Extractions

### Understanding WordPress Architecture

WordPress extraction connects to MySQL databases via SSH tunnels.

**Data Flow**:
1. Fetch site list from Sites API
2. For each site:
   - Create SSH tunnel to MySQL server
   - Extract from WordPress tables
   - Close tunnel
3. Merge all sites to BigQuery

### Basic WordPress Commands

#### Extract Posts (Incremental)

```bash
python orchestrate.py \
  --source wordpress \
  --tables posts
```

**What it does**:
- Fetches all sites from Sites API
- Extracts posts from each site (last 7 days)
- Merges to `wordpress_data.posts`

#### Extract Multiple Tables

```bash
python orchestrate.py \
  --source wordpress \
  --tables posts postmeta users
```

#### Extract from Specific Sites

```bash
python orchestrate.py \
  --source wordpress \
  --tables posts \
  --sites bnalsprd bnacsaprd
```

**Site codes**: Use shortname from Sites API (e.g., `bnalsprd`, `bnacsaprd`)

### WordPress Extraction Modes

#### Incremental (Default)

```bash
python orchestrate.py --source wordpress --tables posts
```

**Behavior**:
- Looks back 7 days from last extraction
- Uses `post_modified >= '2025-01-14'` filter
- Fast for daily updates

#### Full Refresh

```bash
python orchestrate.py --source wordpress --tables posts --refresh full
```

**Behavior**:
- Extracts ALL posts (no date filter)
- Can take hours for large sites
- Use monthly

#### Rebuild Mode

```bash
python orchestrate.py --source wordpress --tables posts --refresh full --rebuild
```

**Behavior**:
- Extracts all data
- Discovers schema from MySQL
- **DROPS and RECREATES** production table

### WordPress Examples

#### Example 1: Daily Posts Update

```bash
# Run daily via cron
python orchestrate.py \
  --source wordpress \
  --tables posts postmeta \
  --log-level info
```

**Crontab entry**:
```cron
# Run at 7 AM daily
0 7 * * * cd /path/to/orchestrator && python orchestrate.py --source wordpress --tables posts postmeta >> /var/log/wordpress_daily.log 2>&1
```

#### Example 2: Single Site Extraction

```bash
# Extract from one site only
python orchestrate.py \
  --source wordpress \
  --tables posts \
  --sites bnalsprd
```

#### Example 3: Full Site Refresh

```bash
# Complete refresh of all tables for all sites
python orchestrate.py \
  --source wordpress \
  --tables posts postmeta users comments \
  --refresh full
```

**Runtime**: Can take 2-4 hours depending on data volume

#### Example 4: New Table Setup

```bash
# First-time extraction of comments table
python orchestrate.py \
  --source wordpress \
  --tables comments \
  --refresh full \
  --rebuild
```

#### Example 5: Test SSH Connection

```bash
# Test extraction without BigQuery changes
python orchestrate.py \
  --source wordpress \
  --tables posts \
  --sites bnalsprd \
  --extract-only \
  --keep-local
```

### WordPress SSH Configuration

SSH keys are configured per-site in YAML:

**configs/wordpress.yaml**:
```yaml
sites:
  bnalsprd:
    ssh_host: bnals-db.example.com
    ssh_port: 22
    ssh_user: deploy
    ssh_key_path: ${WP_SSH_KEY_PATH}  # From .env
    mysql_host: localhost
    mysql_port: 3306
    mysql_database: bnals_wordpress
    mysql_user: readonly
    mysql_password: ${WP_MYSQL_PASSWORD}
```

**Testing SSH**:
```bash
# Test SSH connection
ssh -i /path/to/ssh_key deploy@bnals-db.example.com

# Test MySQL over SSH
ssh -i /path/to/ssh_key deploy@bnals-db.example.com 'mysql -u readonly -p bnals_wordpress -e "SELECT COUNT(*) FROM wp_posts"'
```

---

## Command Reference

### Required Arguments

| Argument | Description | Example |
|----------|-------------|---------|
| `--source` | Data source name | `facebook`, `wordpress` |
| `--tables` | Tables to extract (space-separated) | `campaigns adsets ads` |

### Optional Arguments

| Argument | Description | Default | Example |
|----------|-------------|---------|---------|
| `--sites` | Sites/accounts to extract | All from YAML | `act_123456 act_789012` |
| `--refresh` | Extraction mode | `incremental` | `full`, `incremental` |
| `--rebuild` | Drop and recreate table | `False` | `--rebuild` |
| `--start-date` | Start date for insights | Last extraction | `2025-01-01` |
| `--end-date` | End date for insights | Today | `2025-01-21` |
| `--env` | Environment | `prod` | `staging`, `dev` |
| `--log-level` | Logging verbosity | `INFO` | `DEBUG`, `WARNING` |

### Pipeline Control Arguments

| Argument | Description | Use Case |
|----------|-------------|----------|
| `--extract-only` | Extract to local files only (no GCS/BigQuery) | Testing extraction logic |
| `--skip-gcs` | Skip GCS upload | Local testing |
| `--skip-external-tables` | Skip external table creation | GCS upload only |
| `--skip-transform` | Skip production merge | Staging only |
| `--keep-local` | Keep local Parquet files | Debugging |

### Advanced Arguments

| Argument | Description | Example |
|----------|-------------|---------|
| `--site-filter` | Use only specified sites (ignore YAML) | `--sites site1 --site-filter` |
| `--retry-job` | Retry failed job by ID | `--retry-job JOB_123` |
| `--resume` | Resume from last checkpoint | `--resume` |

### Full Command Syntax

```bash
python orchestrate.py \
  --source {facebook|wordpress} \
  --tables TABLE1 [TABLE2 ...] \
  [--sites SITE1 [SITE2 ...]] \
  [--refresh {incremental|full}] \
  [--rebuild] \
  [--start-date YYYY-MM-DD] \
  [--end-date YYYY-MM-DD] \
  [--env {prod|staging|dev}] \
  [--log-level {DEBUG|INFO|WARNING|ERROR}] \
  [--extract-only] \
  [--skip-gcs] \
  [--skip-external-tables] \
  [--skip-transform] \
  [--keep-local] \
  [--site-filter] \
  [--retry-job JOB_ID] \
  [--resume]
```

---

## Monitoring Jobs

### Real-time Monitoring

**View logs during execution**:
```bash
# Run with verbose logging
python orchestrate.py \
  --source facebook \
  --tables campaigns \
  --log-level debug

# Pipe to file
python orchestrate.py \
  --source facebook \
  --tables campaigns \
  2>&1 | tee facebook_campaigns.log
```

### Job History (BigQuery)

```sql
-- Recent jobs
SELECT
  job_id,
  source,
  job_type,
  status,
  created_at,
  execution_time_seconds,
  rows_extracted,
  rows_inserted,
  rows_updated
FROM orchestrator_monitoring.jobs
WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY created_at DESC
LIMIT 20
```

### Failed Jobs

```sql
-- Failed jobs with errors
SELECT
  job_id,
  source,
  created_at,
  error_message,
  log_file_path
FROM orchestrator_monitoring.jobs
WHERE status = 'failed'
  AND created_at >= CURRENT_DATE()
ORDER BY created_at DESC
```

### Execution Statistics

```sql
-- Average execution time by source/table
SELECT
  source,
  job_type,
  COUNT(*) as job_count,
  AVG(execution_time_seconds) as avg_seconds,
  AVG(rows_extracted) as avg_rows
FROM orchestrator_monitoring.jobs
WHERE status = 'completed'
  AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY source, job_type
ORDER BY source, job_type
```

### Incremental State

```sql
-- Last extraction time per table
SELECT
  source,
  table_name,
  site,
  last_extracted_at,
  last_job_id,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_extracted_at, HOUR) as hours_since
FROM orchestrator_monitoring.extraction_state
ORDER BY last_extracted_at DESC
```

---

## Troubleshooting

### No Data Extracted (0 rows)

**Symptoms**:
```
No data extracted - skipping remaining pipeline steps
```

**Causes**:
1. Incremental lookback window is too short
2. No new/changed data since last run
3. Date filters exclude all data

**Solutions**:
```bash
# Force full refresh
python orchestrate.py --source facebook --tables campaigns --refresh full

# Check last extraction time
SELECT last_extracted_at
FROM orchestrator_monitoring.extraction_state
WHERE source = 'facebook' AND table_name = 'campaigns'

# Extract with specific date range
python orchestrate.py \
  --source facebook \
  --tables campaigns \
  --start-date 2025-01-01 \
  --end-date 2025-01-21
```

### Facebook Rate Limit Errors

**Symptoms**:
```
Rate limit hit for act_123456 (error code 80004). Waiting 5.0 minutes before retry 1/4
```

**Behavior**: Automatic retry with increasing backoff:
- Retry 1: Wait 5 minutes
- Retry 2: Wait 15 minutes
- Retry 3: Wait 30 minutes
- Retry 4: Wait 60 minutes

**Solutions**:
- Let it retry automatically (up to 1 hour wait)
- Reduce batch size in `configs/facebook.yaml`
- Spread extractions across different times

### Schema Mismatch Errors

**Symptoms**:
```
Column 'new_field' not found in production table
Type mismatch: column 'budget' is STRING in source, INT64 in target
```

**Solutions**:
```bash
# Option 1: Let schema evolution handle it (automatic)
# New columns are added automatically as NULLABLE

# Option 2: Rebuild table with correct schema
python orchestrate.py \
  --source facebook \
  --tables campaigns \
  --rebuild

# Option 3: Manual schema update in YAML
# Edit configs/facebook.yaml, add column definition
```

### GCS Upload Failed

**Symptoms**:
```
GCS bucket name not configured - skipping GCS upload
Cannot access GCS bucket my-bucket - skipping upload
```

**Solutions**:
```bash
# Check environment variable
echo $GCS_BUCKET

# Set in .env file
GCS_BUCKET=my-data-lake-bucket

# Test GCS access
gsutil ls gs://my-data-lake-bucket

# Check service account permissions
gcloud projects get-iam-policy bi-data-391216 \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:*"
```

### SSH Connection Failed (WordPress)

**Symptoms**:
```
SSH connection failed to bnals-db.example.com
Connection refused on port 22
```

**Solutions**:
```bash
# Test SSH manually
ssh -i /path/to/ssh_key deploy@bnals-db.example.com

# Check SSH key permissions
chmod 600 /path/to/ssh_key

# Verify SSH key path in .env
WP_SSH_KEY_PATH=/path/to/ssh_key

# Test from Python
python -c "
from shared.config_loader import load_config
config = load_config('wordpress')
print(config.get('sites', {}).get('bnalsprd', {}))
"
```

### BigQuery Permission Denied

**Symptoms**:
```
403 Forbidden: Access Denied: Dataset bi-data-391216:facebook_data
```

**Solutions**:
```bash
# Check current project
gcloud config get-value project

# Set correct project
gcloud config set project bi-data-391216

# Grant dataset permissions
bq update --dataset \
  --add_access_entry role=WRITER,serviceAccount=SERVICE_ACCOUNT_EMAIL \
  facebook_data
```

### Memory Errors (Large Extractions)

**Symptoms**:
```
MemoryError: Unable to allocate array
```

**Solutions**:
```bash
# Extract smaller chunks
python orchestrate.py --source wordpress --tables posts --sites bnalsprd

# Use incremental instead of full
python orchestrate.py --source wordpress --tables posts

# Increase system memory
# Or run on larger machine
```

---

## Best Practices

### Development & Testing

1. **Start with `--extract-only`**:
   ```bash
   python orchestrate.py --source facebook --tables campaigns --extract-only
   ```
   - Tests extraction logic
   - No GCS/BigQuery changes
   - Fast iteration

2. **Use `--keep-local` for debugging**:
   ```bash
   python orchestrate.py --source facebook --tables campaigns --keep-local
   ```
   - Preserves Parquet files in `/tmp/`
   - Inspect data with pandas/DuckDB

3. **Test with single site first**:
   ```bash
   python orchestrate.py --source wordpress --tables posts --sites bnalsprd
   ```

4. **Use `--log-level debug` for troubleshooting**:
   ```bash
   python orchestrate.py --source facebook --tables campaigns --log-level debug
   ```

### Production Operations

1. **Daily Incremental Updates**:
   ```bash
   # Cron: 6 AM daily
   python orchestrate.py --source facebook --tables campaigns adsets ads
   ```

2. **Weekly Full Refresh**:
   ```bash
   # Cron: Sunday 2 AM
   python orchestrate.py --source facebook --tables campaigns --refresh full
   ```

3. **Monitor Job History**:
   ```sql
   -- Check recent failures
   SELECT * FROM orchestrator_monitoring.jobs
   WHERE status = 'failed' AND created_at >= CURRENT_DATE()
   ```

4. **Set Up Alerts**:
   - Configure email notifications in `configs/alerting.yaml`
   - Monitor job execution times
   - Alert on failures

5. **Document Custom Schedules**:
   ```bash
   # Document in crontab comments
   # Facebook campaigns: Daily at 6 AM
   0 6 * * * /path/to/run_facebook.sh
   ```

### Data Quality

1. **Verify Row Counts**:
   ```sql
   -- Compare before/after
   SELECT COUNT(*) FROM facebook_data.campaigns
   ```

2. **Check for Duplicates**:
   ```sql
   -- Should return 0
   SELECT id, account_id, COUNT(*)
   FROM facebook_data.campaigns
   GROUP BY id, account_id
   HAVING COUNT(*) > 1
   ```

3. **Monitor Schema Changes**:
   ```bash
   # Review logs for schema evolution
   grep "Schema evolution" /var/log/facebook_daily.log
   ```

4. **Validate Incremental State**:
   ```sql
   -- Ensure last_extracted_at is recent
   SELECT * FROM orchestrator_monitoring.extraction_state
   WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_extracted_at, HOUR) > 48
   ```

### Performance Optimization

1. **Use Incremental for Daily Updates**:
   - 10x faster than full refresh
   - Reduces API calls
   - Lower BigQuery costs

2. **Schedule Large Jobs Off-Peak**:
   ```bash
   # Run full refresh at 2 AM
   0 2 * * 0 python orchestrate.py --source facebook --tables campaigns --refresh full
   ```

3. **Parallelize Multi-Account Extraction**:
   ```bash
   # Run multiple ad accounts in parallel
   python orchestrate.py --source facebook --tables campaigns --sites act_111111 act_222222 act_333333
   ```

4. **Use Site Filtering for Testing**:
   ```bash
   # Test on one site before running all
   python orchestrate.py --source wordpress --tables posts --sites bnalsprd
   ```

---

## Common Workflows

### Workflow 1: Daily Production Updates

```bash
#!/bin/bash
# daily_update.sh

# Facebook
python orchestrate.py \
  --source facebook \
  --tables campaigns adsets ads \
  --log-level info \
  >> /var/log/facebook_daily.log 2>&1

# WordPress
python orchestrate.py \
  --source wordpress \
  --tables posts postmeta \
  --log-level info \
  >> /var/log/wordpress_daily.log 2>&1
```

**Cron**:
```cron
0 6 * * * /path/to/daily_update.sh
```

### Workflow 2: Weekly Insights Update

```bash
#!/bin/bash
# weekly_insights.sh

# Calculate date range (last 7 days)
END_DATE=$(date +%Y-%m-%d)
START_DATE=$(date -d "7 days ago" +%Y-%m-%d)

# Extract insights
python orchestrate.py \
  --source facebook \
  --tables campaign_insights adset_insights ad_insights \
  --start-date $START_DATE \
  --end-date $END_DATE \
  --refresh full \
  >> /var/log/facebook_insights_weekly.log 2>&1
```

**Cron**:
```cron
0 3 * * 0 /path/to/weekly_insights.sh
```

### Workflow 3: Monthly Full Refresh

```bash
#!/bin/bash
# monthly_refresh.sh

# Full refresh all tables
python orchestrate.py \
  --source facebook \
  --tables campaigns adsets ads \
  --refresh full \
  >> /var/log/facebook_monthly.log 2>&1

python orchestrate.py \
  --source wordpress \
  --tables posts postmeta users \
  --refresh full \
  >> /var/log/wordpress_monthly.log 2>&1
```

**Cron**:
```cron
0 1 1 * * /path/to/monthly_refresh.sh
```

### Workflow 4: Add New Table

```bash
# Step 1: Add table to YAML config
# Edit configs/facebook.yaml, add table definition

# Step 2: First extraction with rebuild
python orchestrate.py \
  --source facebook \
  --tables new_table \
  --refresh full \
  --rebuild \
  --log-level debug

# Step 3: Verify data
bq query "SELECT COUNT(*) FROM facebook_data.new_table"

# Step 4: Add to daily cron
# Edit crontab, add new_table to --tables list
```

---

## Quick Reference

### Most Common Commands

```bash
# Facebook incremental (default)
python orchestrate.py --source facebook --tables campaigns

# Facebook full refresh
python orchestrate.py --source facebook --tables campaigns --refresh full

# Facebook rebuild table
python orchestrate.py --source facebook --tables campaigns --refresh full --rebuild

# WordPress incremental
python orchestrate.py --source wordpress --tables posts

# WordPress single site
python orchestrate.py --source wordpress --tables posts --sites bnalsprd

# Test extraction only
python orchestrate.py --source facebook --tables campaigns --extract-only
```

### Debug Commands

```bash
# Verbose logging
python orchestrate.py --source facebook --tables campaigns --log-level debug

# Keep local files
python orchestrate.py --source facebook --tables campaigns --keep-local

# Skip GCS (local only)
python orchestrate.py --source facebook --tables campaigns --skip-gcs

# Extract only (no BigQuery)
python orchestrate.py --source facebook --tables campaigns --extract-only
```

### Monitoring Queries

```sql
-- Recent jobs
SELECT * FROM orchestrator_monitoring.jobs ORDER BY created_at DESC LIMIT 10

-- Failed jobs
SELECT * FROM orchestrator_monitoring.jobs WHERE status = 'failed' AND created_at >= CURRENT_DATE()

-- Extraction state
SELECT * FROM orchestrator_monitoring.extraction_state ORDER BY last_extracted_at DESC
```

---

## Workflow Automation & Job Chaining

### Overview

Job chaining allows you to automatically execute downstream jobs based on completion status. Use `--on-success`, `--on-failure`, or `--on-finish` to create automated workflows.

### Basic Usage

**Execute on success**:
```bash
python orchestrate.py --source facebook --env prod --tables pages \
  --on-success "python orchestrate.py --source facebook --env prod --tables posts"
```

**Execute on failure**:
```bash
python orchestrate.py --source wordpress --env prod --tables posts \
  --on-failure "python scripts/send_alert.py"
```

**Execute always (regardless of status)**:
```bash
python orchestrate.py --source facebook --env prod --tables campaigns \
  --on-finish "python scripts/cleanup_temp.py"
```

### Multi-Step Workflows

**Extract → Transform → Load**:
```bash
python orchestrate.py --source facebook --env prod --tables campaigns \
  --on-success "python transform_campaigns.py" \
  --on-finish "python send_report.py"
```

**Complex workflow with error handling**:
```bash
python orchestrate.py --source facebook --env prod --tables pages \
  --on-success "python orchestrate.py --source facebook --env prod --tables posts" \
  --on-failure "python scripts/alert_failure.py" \
  --on-finish "python scripts/cleanup.py"
```

### Job Lineage Tracking

**Pass job ID to child jobs**:
```bash
python orchestrate.py --source facebook --env prod --tables pages \
  --pass-job-id \
  --on-success "python orchestrate.py --source facebook --env prod --tables posts"
```

**Query lineage in BigQuery**:
```sql
-- Find child jobs
SELECT
  job_id, source, tables, status, created_at
FROM `orchestrator_monitoring.jobs`
WHERE parent_job_id = 'abc-123-def-456'

-- Job family tree
WITH RECURSIVE job_tree AS (
  SELECT job_id, parent_job_id, source, tables, 0 as level
  FROM `orchestrator_monitoring.jobs`
  WHERE job_id = 'root-job-id'

  UNION ALL

  SELECT j.job_id, j.parent_job_id, j.source, j.tables, jt.level + 1
  FROM `orchestrator_monitoring.jobs` j
  JOIN job_tree jt ON j.parent_job_id = jt.job_id
)
SELECT * FROM job_tree ORDER BY level, created_at
```

### Advanced Features

**Pass execution ID**:
```bash
python orchestrate.py --source facebook --env prod --tables campaigns \
  --pass-execution-id \
  --on-success "python next_step.py"
```

The chained command receives `--execution-id <id>` automatically.

**Parameter passing**:
When using `--pass-job-id` or `--pass-execution-id`, the following are appended to chained commands:
- `--parent-job-id <job_id>` (if `--pass-job-id`)
- `--execution-id <exec_id>` (if `--pass-execution-id`)
- `--parent-status <status>` (always appended: COMPLETED/FAILED/WARNING)

### Complete Documentation

See [JOB_CHAINING_GUIDE.md](../JOB_CHAINING_GUIDE.md) for:
- 8 detailed examples
- Error handling best practices
- Security considerations
- Job workflow patterns

---

**End of User Manual**

**Related Documentation**:
- [JOB_CHAINING_GUIDE.md](../JOB_CHAINING_GUIDE.md) - Workflow automation
- [PIPELINE_MANAGER_MANUAL.md](PIPELINE_MANAGER_MANUAL.md) - Job monitoring CLI
- [DATA_FLOW_MANUAL.md](DATA_FLOW_MANUAL.md) - Architecture details
- [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md) - Development guide
