# Post-Processing SQL Feature

The orchestrator supports running custom SQL files after pipeline execution to create BI-optimized views, aggregations, and other derived tables.

## Table of Contents

- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Template Variables](#template-variables)
- [Examples](#examples)
- [Command Line Options](#command-line-options)
- [Best Practices](#best-practices)

## Quick Start

### 1. Create a SQL file

```sql
-- sql/mailchimp/bi_campaign_performance.sql
CREATE OR REPLACE TABLE `{project_id}.{bi_dataset}.campaign_performance`
PARTITION BY DATE(send_time)
CLUSTER BY list_id, campaign_id
AS
SELECT
  campaign_id,
  send_time,
  emails_sent,
  opens_unique,
  ROUND(SAFE_DIVIDE(opens_unique, emails_sent) * 100, 2) as open_rate_pct,
  CURRENT_TIMESTAMP() as refreshed_at
FROM `{project_id}.{production_dataset}.mailchimp_campaigns`
WHERE status = 'sent';
```

### 2. Add to your YAML config

```yaml
# configs/mailchimp.yaml
pipeline:
  production_dataset: mailchimp_data
  bi_dataset: mailchimp_bi

  post_process:
    - sql_file: sql/mailchimp/bi_campaign_performance.sql
      description: "Create campaign performance BI view"
      enabled: true
```

### 3. Run your pipeline

```bash
python orchestrate.py --source mailchimp --env prod --lookback 7
```

The post-processing SQL will run automatically after the pipeline completes successfully.

## Configuration

Post-processing can be configured at three levels:

### Pipeline Level (Most Common)

Runs after ALL tables in the pipeline complete:

```yaml
# configs/mailchimp.yaml
pipeline:
  production_dataset: mailchimp_data
  bi_dataset: mailchimp_bi

  post_process:
    - sql_file: sql/mailchimp/bi_campaign_performance.sql
      description: "Campaign performance BI view"
      enabled: true

    - sql_file: sql/mailchimp/bi_member_engagement.sql
      description: "Member engagement rollup"
      enabled: true

    - sql_file: sql/mailchimp/bi_experimental.sql
      description: "Experimental metrics (disabled)"
      enabled: false
```

### Table Level

Runs after a specific table completes:

```yaml
resources:
  campaigns:
    table_name: mailchimp_campaigns
    primary_key: [campaign_id]

    post_process:
      - sql_file: sql/mailchimp/campaigns_enrichment.sql
        description: "Enrich campaigns with calculated fields"
        enabled: true
```

### Group Level

Runs after a table group completes:

```yaml
table_groups:
  campaign_group:
    tables: [campaign_email_activity, campaign_sent_to, ...]

    post_process:
      - sql_file: sql/mailchimp/campaign_group_rollup.sql
        description: "Campaign analytics rollup"
        enabled: true
```

## Configuration Formats

### Detailed Format (Recommended)

Full control with description and enable/disable:

```yaml
post_process:
  - sql_file: sql/mailchimp/bi_campaign_performance.sql
    description: "Campaign performance BI view"
    enabled: true
```

**Fields:**
- `sql_file` (required): Path to SQL file (relative to project root)
- `description` (optional): Human-readable description for logs (defaults to sql_file path)
- `enabled` (optional): Whether to run this SQL file (defaults to true)

### Simple Format

Shorthand for enabled SQL files:

```yaml
post_process:
  - sql/mailchimp/bi_campaign_performance.sql
  - sql/mailchimp/bi_member_engagement.sql
```

Equivalent to:

```yaml
post_process:
  - sql_file: sql/mailchimp/bi_campaign_performance.sql
    enabled: true
  - sql_file: sql/mailchimp/bi_member_engagement.sql
    enabled: true
```

### Mixed Format

Combine both formats:

```yaml
post_process:
  # Simple format
  - sql/mailchimp/bi_campaign_performance.sql

  # Detailed format with options
  - sql_file: sql/mailchimp/bi_experimental.sql
    description: "Experimental A/B test metrics"
    enabled: false
```

## Template Variables

All SQL files support template variable substitution using Python's `.format()` syntax:

| Variable | Description | Example |
|----------|-------------|---------|
| `{project_id}` | GCP project ID | `bionews-data-warehouse` |
| `{production_dataset}` | Production dataset name | `mailchimp_data` |
| `{staging_dataset}` | Staging dataset name | `mailchimp_staging` |
| `{bi_dataset}` | BI dataset name | `mailchimp_bi` |
| `{execution_id}` | Current job execution ID | `mailchimp_20251119_123456` |
| `{source}` | Data source name | `mailchimp` |

**Usage in SQL:**

```sql
SELECT * FROM `{project_id}.{production_dataset}.mailchimp_campaigns`
WHERE execution_id = '{execution_id}'
```

**Becomes:**

```sql
SELECT * FROM `bionews-data-warehouse.mailchimp_data.mailchimp_campaigns`
WHERE execution_id = 'mailchimp_20251119_123456'
```

## Examples

### Example 1: Campaign Performance BI View

```sql
-- sql/mailchimp/bi_campaign_performance.sql
CREATE SCHEMA IF NOT EXISTS `{project_id}.{bi_dataset}`;

CREATE OR REPLACE TABLE `{project_id}.{bi_dataset}.campaign_performance`
PARTITION BY DATE(send_time)
CLUSTER BY list_id, campaign_type, campaign_id
OPTIONS (
  description = "Campaign performance metrics",
  partition_expiration_days = 365,
  require_partition_filter = true
)
AS
SELECT
  -- Identifiers
  c.campaign_id,
  c.campaign_name,
  c.list_id,
  l.list_name,

  -- Date dimensions
  c.send_time,
  DATE(c.send_time) as send_date,
  EXTRACT(YEAR FROM c.send_time) as year,
  EXTRACT(MONTH FROM c.send_time) as month,

  -- Metrics
  c.emails_sent,
  c.opens_unique,
  c.clicks_unique,

  -- Calculated rates
  ROUND(SAFE_DIVIDE(c.opens_unique, c.emails_sent) * 100, 2) as open_rate_pct,
  ROUND(SAFE_DIVIDE(c.clicks_unique, c.emails_sent) * 100, 2) as click_rate_pct,

  -- Metadata
  CURRENT_TIMESTAMP() as refreshed_at,
  '{execution_id}' as execution_id

FROM `{project_id}.{production_dataset}.mailchimp_campaigns` c
LEFT JOIN `{project_id}.{production_dataset}.mailchimp_lists` l
  ON c.list_id = l.list_id

WHERE c.status = 'sent'
  AND c.send_time >= '2024-01-01';

-- Log results
SELECT
  COUNT(*) as campaigns_processed,
  MIN(send_date) as earliest_campaign,
  MAX(send_date) as latest_campaign
FROM `{project_id}.{bi_dataset}.campaign_performance`;
```

### Example 2: Multiple Statements

SQL files can contain multiple statements separated by semicolons:

```sql
-- sql/facebook/bi_ad_performance.sql

-- Statement 1: Create dataset
CREATE SCHEMA IF NOT EXISTS `{project_id}.{bi_dataset}`;

-- Statement 2: Create daily performance table
CREATE OR REPLACE TABLE `{project_id}.{bi_dataset}.ad_performance_daily`
PARTITION BY report_date
AS
SELECT
  DATE(date_start) as report_date,
  campaign_id,
  ad_id,
  impressions,
  clicks,
  spend
FROM `{project_id}.{production_dataset}.facebook_ad_insights`
WHERE date_start >= '2024-01-01';

-- Statement 3: Create monthly rollup
CREATE OR REPLACE TABLE `{project_id}.{bi_dataset}.ad_performance_monthly`
AS
SELECT
  DATE_TRUNC(report_date, MONTH) as month,
  campaign_id,
  SUM(impressions) as total_impressions,
  SUM(clicks) as total_clicks,
  SUM(spend) as total_spend
FROM `{project_id}.{bi_dataset}.ad_performance_daily`
GROUP BY 1, 2;

-- Statement 4: Log results
SELECT 'Processing complete' as status, COUNT(*) as rows
FROM `{project_id}.{bi_dataset}.ad_performance_monthly`;
```

### Example 3: Conditional Execution

Use YAML to control which SQL files run:

```yaml
# configs/mailchimp.yaml
pipeline:
  post_process:
    # Always run - core BI views
    - sql_file: sql/mailchimp/bi_campaign_performance.sql
      enabled: true

    - sql_file: sql/mailchimp/bi_member_engagement.sql
      enabled: true

    # Seasonal - enable only during holiday period
    - sql_file: sql/mailchimp/bi_holiday_analysis.sql
      description: "Holiday campaign analysis (Nov-Dec only)"
      enabled: false

    # Development - disable in production
    - sql_file: sql/mailchimp/bi_experimental_metrics.sql
      description: "Experimental metrics under development"
      enabled: false
```

## Command Line Options

### Skip Post-Processing

Skip all post-processing SQL execution:

```bash
python orchestrate.py --source mailchimp --env prod --skip-post-process
```

### Extract-Only Mode

Post-processing is automatically skipped in extract-only mode:

```bash
python orchestrate.py --source mailchimp --env prod --extract-only
# Post-processing will NOT run
```

## Execution Behavior

### Success Case

```
[2025-11-19 14:30:45] INFO: Pipeline completed successfully in 45.23 seconds

================================================================================
POST-PROCESSING PHASE STARTING
================================================================================
[2025-11-19 14:30:45] INFO: Running post-process: Campaign performance BI view
[2025-11-19 14:30:45] INFO: Executing post-process SQL: sql/mailchimp/bi_campaign_performance.sql
[2025-11-19 14:30:47] INFO: ✓ Post-process SQL completed: sql/mailchimp/bi_campaign_performance.sql
[2025-11-19 14:30:47] INFO: Running post-process: Member engagement rollup
[2025-11-19 14:30:47] INFO: Executing post-process SQL: sql/mailchimp/bi_member_engagement.sql
[2025-11-19 14:30:49] INFO: ✓ Post-process SQL completed: sql/mailchimp/bi_member_engagement.sql
[2025-11-19 14:30:49] INFO: ✓ Post-processing: 2/2 succeeded
================================================================================
```

### With Disabled Files

```
[2025-11-19 14:30:45] INFO: Running post-process: Campaign performance BI view
[2025-11-19 14:30:45] INFO: Executing post-process SQL: sql/mailchimp/bi_campaign_performance.sql
[2025-11-19 14:30:47] INFO: ✓ Post-process SQL completed: sql/mailchimp/bi_campaign_performance.sql
[2025-11-19 14:30:47] INFO: Skipping disabled post-process: Experimental A/B test metrics
[2025-11-19 14:30:47] INFO: Running post-process: Member engagement rollup
[2025-11-19 14:30:47] INFO: Executing post-process SQL: sql/mailchimp/bi_member_engagement.sql
[2025-11-19 14:30:49] INFO: ✓ Post-process SQL completed: sql/mailchimp/bi_member_engagement.sql
[2025-11-19 14:30:49] INFO: ✓ Post-processing: 2/2 succeeded
```

### Error Handling

If a post-processing SQL file fails:
- Error is logged
- Other post-processing files continue to execute
- Pipeline is marked as successful (post-processing failure doesn't fail the pipeline)

```
[2025-11-19 14:30:47] ERROR: Post-process failed: Campaign performance BI view - Table not found
[2025-11-19 14:30:47] WARNING: Pipeline succeeded but post-processing encountered errors
```

## Best Practices

### 1. Use CTAS for BI Views

CREATE OR REPLACE TABLE is simpler and often cheaper than incremental merges for BI use cases:

```sql
CREATE OR REPLACE TABLE `{project_id}.{bi_dataset}.view_name`
PARTITION BY date_field
CLUSTER BY key_fields
AS SELECT ...
```

### 2. Always Partition and Cluster

Improve query performance and reduce costs:

```sql
PARTITION BY DATE(send_time)
CLUSTER BY list_id, campaign_type, campaign_id
```

### 3. Filter Historical Data

Only keep relevant date ranges:

```sql
WHERE send_time >= '2024-01-01'
  AND status = 'sent'
```

### 4. Add Metadata Columns

Track when the view was refreshed:

```sql
SELECT
  ...,
  CURRENT_TIMESTAMP() as refreshed_at,
  '{execution_id}' as execution_id
FROM ...
```

### 5. Use SAFE_DIVIDE

Prevent division by zero errors:

```sql
ROUND(SAFE_DIVIDE(opens_unique, emails_sent) * 100, 2) as open_rate_pct
```

### 6. Set Table Options

Add descriptions and expiration:

```sql
OPTIONS (
  description = "Campaign performance metrics for BI",
  partition_expiration_days = 365,
  require_partition_filter = true,
  labels = [("source", "mailchimp"), ("pipeline", "bi")]
)
```

### 7. Log Results

End with a SELECT to verify execution:

```sql
-- Log results
SELECT
  COUNT(*) as row_count,
  MIN(send_date) as earliest,
  MAX(send_date) as latest
FROM `{project_id}.{bi_dataset}.campaign_performance`;
```

### 8. Use Descriptive Filenames

Make it easy to understand what each SQL file does:

```
sql/
├── mailchimp/
│   ├── bi_campaign_performance.sql
│   ├── bi_member_engagement.sql
│   └── bi_list_health.sql
```

### 9. Document Your SQL

Add comments explaining the purpose:

```sql
-- Post-processing SQL for campaign performance BI view
-- Creates a partitioned, clustered table optimized for BI queries
-- Includes pre-calculated engagement rates and date dimensions
```

### 10. Test Separately

Test SQL files independently before adding to pipeline:

```bash
# Test template substitution
cat sql/mailchimp/bi_campaign_performance.sql | \
  sed 's/{project_id}/bionews-data-warehouse/g' | \
  sed 's/{production_dataset}/mailchimp_data/g' | \
  sed 's/{bi_dataset}/mailchimp_bi/g' | \
  bq query --use_legacy_sql=false
```

## Troubleshooting

### SQL File Not Found

```
ERROR: Post-process SQL file not found: sql/mailchimp/bi_view.sql
```

**Solution:** Check the file path is correct and relative to project root (`c:\orchestrator\`)

### Template Variable Not Substituted

```
ERROR: Table name contains literal {project_id}
```

**Solution:** Ensure you're using the correct variable names (see [Template Variables](#template-variables))

### Post-Processing Not Running

**Check:**
1. Is `enabled: true` (or omitted, which defaults to true)?
2. Is `--skip-post-process` flag used?
3. Is `--extract-only` flag used?
4. Did the pipeline complete successfully?

### SQL Statement Fails

**Check logs:**
```
ERROR: Failed to execute statement 2: Table not found: mailchimp_data.mailchimp_campaigns
```

**Solution:** Verify the production tables exist before post-processing runs

## Directory Structure

```
c:\orchestrator\
├── sql/                                 # Post-processing SQL files
│   ├── README.md                       # SQL documentation
│   ├── test_post_process.sql          # Test file
│   ├── mailchimp/
│   │   ├── bi_campaign_performance.sql
│   │   └── bi_member_engagement.sql
│   ├── facebook/
│   │   └── bi_ad_performance.sql
│   └── limesurvey/
│
├── shared/
│   └── post_processor.py               # Post-processing module
│
├── configs/
│   ├── mailchimp.yaml                  # Contains post_process config
│   ├── facebook.yaml
│   └── limesurvey.yaml
│
└── orchestrate.py                       # Main pipeline script
```

## Related Documentation

- [Mailchimp Pipeline](MAILCHIMP_DATA_MODEL.md)
- [Smart Retry System](SMART_RETRY_SYSTEM.md)
- [Failure Tracking](FAILURE_TRACKING_AND_RETRY.md)
