# Campaign Manager 360 (DCM) Extractor - Quick Start Guide

## Overview

The DCM extractor pulls campaign performance data from Google Campaign Manager 360 using the Reporting API. It supports:

- **Multi-client account extraction** (75+ client accounts with read-only access)
- **Dynamic account discovery** (automatically picks up new client accounts each run)
- **Configurable lookback windows** (3, 7, 30 days for incremental loads)
- **Historical and incremental loads**
- **Multiple report types** (Standard, Floodlight, Path to Conversion)
- **API-only approach** (no DTS dependency)

## Important: Client Account Model

**This extractor is designed for agencies/service providers who have read-only access to multiple CLIENT accounts.**

- You do NOT own these DCM accounts
- You've been granted "Reporting" access by clients
- New clients = automatic discovery (no code changes needed)
- DTS (Data Transfer Service) is NOT available for this use case
- All data extraction via Reporting API only

## Prerequisites

### 1. Google Cloud Service Account

You need a service account with access to DCM accounts:

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create or select a project
3. Navigate to **IAM & Admin → Service Accounts**
4. Click **Create Service Account**
5. Name it (e.g., `dcm-extractor`)
6. Grant roles:
   - **Campaign Manager 360 API User** (if available)
   - Or custom role with `dfareporting.*` permissions
7. Click **Create Key** → **JSON**
8. Download and save securely

### 2. Grant Service Account Access in DCM

For EACH DCM account you want to extract from:

1. Log into [Campaign Manager 360](https://campaignmanager.google.com/)
2. Navigate to **Admin → User Profiles**
3. Click **New Profile** or edit existing
4. Add your service account email: `dcm-extractor@your-project.iam.gserviceaccount.com`
5. Grant **Reporting** permissions (minimum)
6. Save

**You must do this for all 75+ accounts!** (Or have your DCM admin do it)

### 3. Environment Variables

Set these environment variables:

```bash
# DCM Service Account
export DCM_SERVICE_ACCOUNT_KEY="/path/to/service-account-key.json"

# BigQuery Destination
export GCP_PROJECT_ID="your-gcp-project-id"

# Google Application Credentials (for BigQuery)
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

Or on Windows:
```cmd
set DCM_SERVICE_ACCOUNT_KEY=C:\path\to\service-account-key.json
set GCP_PROJECT_ID=your-gcp-project-id
set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\service-account-key.json
```

## Configuration

### Basic Configuration

Edit `configs/dcm.yaml`:

```yaml
source:
  connection:
    service_account_key: ${DCM_SERVICE_ACCOUNT_KEY}

  # Extract from all accessible accounts
  accounts: all

  date_range:
    mode: incremental
    incremental:
      lookback_days: 7  # 7-day lookback for attribution updates

resources:
  campaign_performance:
    enabled: true
    table: dcm_campaign_performance

destination:
  bigquery:
    project_id: ${GCP_PROJECT_ID}
    dataset: dcm_data
```

### Account Selection Options

**Option 1: All Accounts (RECOMMENDED for Client Management)**
```yaml
accounts: all
```

**How it works:**
1. At the start of each ETL job, calls `userprofiles().list()` API
2. Discovers ALL profiles your service account can access
3. Automatically includes new client accounts (no config changes!)
4. Saves discovered list to `dcm_discovered_accounts.json` for audit trail

**Why this is important:**
- Client adds your service account → automatically picked up next run
- Client removes access → automatically excluded
- No manual config updates when onboarding/offboarding clients

**Example output:**
```
============================================================
DISCOVERING DCM ACCOUNTS (dynamic API lookup)
============================================================
✓ Discovered 75 accessible DCM profiles
  - Profile 7560911: Adswerve - 3 - DCM - US 9515 (Account: 123456)
  - Profile 9515: AdOperations.Bionews (Account: 789012)
  ...
✓ Saved discovered accounts to dcm_discovered_accounts.json
============================================================
```

**Option 2: Specific Accounts (Testing Only)**
```yaml
accounts: [7560911, 9515, 636]
```
Only process these profile IDs. Use for testing or limiting to specific clients.

**Option 3: Load from JSON (Not Recommended)**
```yaml
accounts: dcm_accounts.json
```
Static list - won't automatically pick up new accounts.

### Date Range Configuration

**Incremental Load with Lookback (Recommended)**
```yaml
date_range:
  mode: incremental
  incremental:
    lookback_days: 7  # Pull last 7 days
```

**Full Historical Load**
```yaml
date_range:
  mode: full
  full:
    start_date: "2022-01-01"
    end_date: "2024-11-17"
```

**Why Use Lookback?**
- Conversions can be attributed retroactively (up to 30 days)
- Data may be updated/corrected 24-72 hours later
- 7-day lookback ensures you capture late attribution

### Adjustable Lookback

Change lookback window for different use cases:

```yaml
# Short lookback (fast, recent data only)
lookback_days: 3

# Standard lookback (recommended for most cases)
lookback_days: 7

# Extended lookback (for conversion-heavy campaigns)
lookback_days: 30
```

## Usage

### 1. Test API Access

First, verify your service account can access DCM:

```bash
python plugins/dcm_extractor.py
```

Expected output:
```
Testing DCM API initialization...
✓ API initialized successfully
✓ Found 75 accessible profiles
  - Profile 7560911: AdOperations.Bionews
  - Profile 9515: adoperations74
  - Profile 636: Acceleration - ACC US - DCM - US
  ...
```

If you see errors, check:
- Service account key path is correct
- Service account has been granted access in DCM UI
- DCM API is enabled in your GCP project

### 2. Run Full Historical Load

```bash
python orchestrate.py --source dcm --env prod --group campaign_performance --mode full
```

This will:
1. Discover all accessible DCM profiles
2. Create a Standard report for each profile
3. Run reports (asynchronous)
4. Poll for completion (can take 5-30 minutes per report)
5. Download CSV files
6. Load to BigQuery

### 3. Run Incremental Load (Daily)

```bash
python orchestrate.py --source dcm --env prod --group campaign_performance --mode incremental
```

With 7-day lookback, this pulls:
- Start date: 8 days ago
- End date: Yesterday

Data is **merged** into BigQuery using the primary key:
```sql
PRIMARY KEY (date, campaign_id, placement_id, ad_id)
```

This handles retroactive attribution updates automatically.

### 4. Test Mode (Single Account)

Process only the first account to test:

```bash
python orchestrate.py --source dcm --env prod --group campaign_performance --test-mode
```

### 5. Validate Only (No Load)

Generate reports but don't load to BigQuery:

```bash
python orchestrate.py --source dcm --env prod --group campaign_performance --validate-only
```

## Report Types

### Standard Campaign Performance

```yaml
campaign_performance:
  enabled: true
  report_type: STANDARD
  dimensions:
    - date
    - advertiser
    - campaign
    - placement
    - ad
  metrics:
    - impressions
    - clicks
    - totalConversions
    - mediaCost
```

**Use for:** Daily campaign performance tracking

### Floodlight Conversions

```yaml
floodlight_conversions:
  enabled: true
  report_type: FLOODLIGHT
  floodlight_config_id: 12345  # Your floodlight config ID
  dimensions:
    - date
    - campaign
    - activity
    - activityGroup
  metrics:
    - totalConversions
    - clickThroughConversions
    - viewThroughConversions
    - totalConversionsRevenue
```

**Use for:** Conversion tracking and revenue analysis

### Path to Conversion

```yaml
path_to_conversion:
  enabled: true
  report_type: PATH_TO_CONVERSION
  floodlight_config_id: 12345
  attribution:
    clicks_lookback_window: 30
    impressions_lookback_window: 1
  dimensions:
    - date
    - campaign
    - pathType
  metrics:
    - totalConversions
    - pathImpressions
    - pathClicks
```

**Use for:** Multi-touch attribution analysis

## API Methods Explained

### Account Discovery

```python
# Get all accessible profiles
client = DCMClient(credentials_path)
client.initialize()
profiles = client.get_user_profiles()

# Returns list of:
# [
#   {"profileId": 7560911, "accountId": ..., "userName": "..."},
#   {"profileId": 9515, "accountId": ..., "userName": "..."},
#   ...
# ]
```

### Report Creation

```python
# Create a Standard report
report_config = {
    "name": "Campaign Performance",
    "type": "STANDARD",
    "criteria": {
        "dateRange": {
            "startDate": "2024-11-10",
            "endDate": "2024-11-17"
        },
        "dimensions": [{"name": "date"}, {"name": "campaign"}],
        "metricNames": ["impressions", "clicks"]
    },
    "format": "CSV"
}

report_id = client.create_report(profile_id, report_config)
```

### Report Execution

```python
# Run the report (asynchronous)
file_id = client.run_report(profile_id, report_id)

# Wait for completion (polls every 30s)
download_url = client.wait_for_report(
    profile_id,
    report_id,
    file_id,
    max_wait_seconds=1800  # 30 minutes
)

# Download CSV
client.download_report(profile_id, report_id, file_id, "output.csv")
```

### Incremental Load with Lookback

```python
from datetime import datetime, timedelta, date

def get_date_range_with_lookback(lookback_days=7):
    end_date = datetime.now().date() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=lookback_days)

    return (
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d")
    )

# 3-day lookback
date_range = get_date_range_with_lookback(3)
# Returns: ("2024-11-14", "2024-11-17")

# 7-day lookback
date_range = get_date_range_with_lookback(7)
# Returns: ("2024-11-10", "2024-11-17")

# 30-day lookback
date_range = get_date_range_with_lookback(30)
# Returns: ("2024-10-18", "2024-11-17")
```

## BigQuery Schema

### campaign_performance Table

```sql
CREATE TABLE dcm_data.dcm_campaign_performance (
  -- Dimensions
  date DATE,
  advertiser STRING,
  advertiser_id INT64,
  campaign STRING,
  campaign_id INT64,
  placement STRING,
  placement_id INT64,
  ad STRING,
  ad_id INT64,
  site STRING,
  site_id INT64,
  state STRING,
  city STRING,
  browser_platform STRING,
  operating_system STRING,
  device_type STRING,

  -- Metrics
  impressions INT64,
  clicks INT64,
  click_rate FLOAT64,
  total_conversions INT64,
  click_through_conversions INT64,
  view_through_conversions INT64,
  total_conversions_revenue FLOAT64,
  media_cost FLOAT64,
  active_view_viewable_impressions INT64,
  active_view_measurable_impressions INT64,
  active_view_eligible_impressions INT64,

  -- Metadata
  _profile_id INT64,
  _account_name STRING,
  _extracted_at TIMESTAMP
)
PARTITION BY date
CLUSTER BY advertiser_id, campaign_id, date;
```

### Deduplication Strategy

When running incremental loads with lookback, use **MERGE** to handle updates:

```sql
-- Orchestrator automatically handles this via WRITE_APPEND + primary_key
-- But for manual merges:

MERGE `project.dcm_data.dcm_campaign_performance` AS target
USING `project.dcm_data.dcm_campaign_performance_staging` AS source
ON target.date = source.date
   AND target.campaign_id = source.campaign_id
   AND target.placement_id = source.placement_id
   AND target.ad_id = source.ad_id
WHEN MATCHED THEN
  UPDATE SET
    impressions = source.impressions,
    clicks = source.clicks,
    total_conversions = source.total_conversions,
    -- ... other fields
WHEN NOT MATCHED THEN
  INSERT ROW;
```

## Scheduling

### Daily Incremental (Recommended)

Run every day at 9am to pull yesterday's data + 7-day lookback:

```bash
# Cron (Linux/Mac)
0 9 * * * cd /path/to/orchestrator && python orchestrate.py --source dcm --env prod --group campaign_performance

# Windows Task Scheduler
# Action: Start a program
# Program: C:\Python\python.exe
# Arguments: orchestrate.py --source dcm --env prod --group campaign_performance
# Start in: C:\orchestrator
# Trigger: Daily at 9:00 AM
```

### Weekly Full Refresh

Run every Sunday to backfill last 30 days:

```yaml
# configs/dcm_weekly.yaml
date_range:
  mode: incremental
  incremental:
    lookback_days: 30
```

```bash
# Cron
0 2 * * 0 cd /path/to/orchestrator && python orchestrate.py --source dcm --env prod --group campaign_performance --config configs/dcm_weekly.yaml
```

## Troubleshooting

### Error: "Failed to initialize DCM API"

**Check:**
1. Service account key file exists and path is correct
2. `DCM_SERVICE_ACCOUNT_KEY` environment variable is set
3. JSON key file is valid (not corrupted)

### Error: "No accessible profiles found"

**Check:**
1. Service account has been granted access in DCM UI
2. You're checking the correct DCM account (not a test account)
3. Permissions include at least "Reporting" access

### Error: "Report timed out"

**Solutions:**
1. Increase `max_wait_seconds` in config:
   ```yaml
   processing:
     report_polling:
       max_wait_seconds: 7200  # 2 hours
   ```
2. Reduce date range (split into smaller chunks)
3. Reduce number of dimensions/metrics

### Error: "Failed to download report"

**Check:**
1. Report status is "REPORT_AVAILABLE" (not "FAILED" or "PROCESSING")
2. Download URL is valid
3. Network connectivity to Google APIs
4. Local disk space available

### No Data in Report

**Possible causes:**
1. No campaigns ran during the date range
2. Filters too restrictive
3. Wrong floodlight_config_id (for Floodlight reports)
4. Account has no data for those dimensions

## Performance Optimization

### Parallel Processing

Process multiple accounts in parallel:

```yaml
processing:
  max_workers: 10  # Process 10 accounts concurrently
```

**Caution:** DCM API has rate limits (10 requests/second). Don't set too high.

### Report Polling

Optimize polling interval:

```yaml
processing:
  report_polling:
    max_wait_seconds: 3600  # 1 hour
    check_interval: 60      # Check every 60 seconds (instead of 30)
```

Longer intervals = fewer API calls, but slower detection of completion.

### Use Saved Reports

Instead of creating new reports each time:

1. Create reports manually in DCM UI
2. Get report IDs from URL
3. Configure:
   ```yaml
   advanced:
     use_saved_reports: true
     saved_report_ids:
       7560911: "12345678"  # profile_id: report_id
       9515: "23456789"
   ```

This skips report creation step, just runs existing reports.

## API Limits

| Limit Type | Value | Notes |
|------------|-------|-------|
| API Requests | 10/second per user | Service account = 1 user |
| Report Runs | 100/day per user | Across all accounts |
| Report Generation | 5-30 minutes | Depends on data volume |
| File Retention | 30 days | Download within 30 days |
| Max File Size | 2 GB | Auto-split if larger |

## Next Steps

1. **Set up service account** - Follow prerequisites
2. **Test API access** - Run `python plugins/dcm_extractor.py`
3. **Configure accounts** - Edit `configs/dcm.yaml`
4. **Run test extraction** - Single account first
5. **Run full extraction** - All accounts
6. **Schedule daily runs** - Cron or Task Scheduler
7. **Monitor BigQuery** - Verify data quality
8. **Request DTS** - For event-level data (optional)

## Support

- **CM360 API Docs**: https://developers.google.com/doubleclick-advertisers
- **BigQuery Docs**: https://cloud.google.com/bigquery/docs
- **Orchestrator Issues**: File issue in repository

---

**Happy Extracting! 🚀**
