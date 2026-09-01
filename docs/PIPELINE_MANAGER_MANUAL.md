# Pipeline Manager User Manual

**Version:** 1.1.0
**Date:** 2025-10-26
**Copyright:** © 2025 Bionews. All Rights Reserved.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Quick Start](#quick-start)
4. [Command Reference](#command-reference)
5. [Common Workflows](#common-workflows)
6. [Troubleshooting](#troubleshooting)
7. [Advanced Usage](#advanced-usage)

---

## Overview

**Pipeline Manager** is a CLI tool for manual intervention and management of data pipeline operations. It provides commands for:

- Job monitoring and cancellation
- Pipeline health checks
- Resource usage analysis
- Cleanup and maintenance
- Dependency management
- Template-based execution

**Location:** `./pipeline_manager.py` (780 lines, 33KB)

**Author:** Data Pipeline Team
**Created:** 2025-01-02

---

## Prerequisites

### Required Environment Variables

Pipeline Manager automatically loads environment variables from the `.env` file in the orchestrator directory.

Ensure your `.env` file exists and contains:

```bash
GCP_PROJECT_ID=bi-data-391216
GOOGLE_APPLICATION_CREDENTIALS=c:\gcp\service-account-bionews-pipeline.json
```

The tool will load these variables automatically on startup. No additional configuration is needed.

### Required Permissions

- BigQuery: `roles/bigquery.admin` or equivalent
- GCS: `roles/storage.objectAdmin` for cleanup operations
- Monitoring dataset access: read/write to `orchestrator_monitoring_prod`

### Dependencies

All dependencies are listed in `requirements.txt`. The tool uses:
- `google-cloud-bigquery`
- `google-cloud-storage`
- Shared modules from `shared/` directory

---

## Quick Start

### Check Pipeline Health

```bash
python pipeline_manager.py health --hours-back 24
```

### List Recent Jobs

```bash
python pipeline_manager.py list-jobs --limit 10
```

### Check Specific Job Status

```bash
python pipeline_manager.py status --job-id job_20250102_001
```

### View Resource Usage

```bash
python pipeline_manager.py resources --hours-back 24
```

---

## Command Reference

### 1. cancel-job

Cancel a running pipeline job.

**Usage:**
```bash
python pipeline_manager.py cancel-job --job-id <JOB_ID> [--reason <REASON>]
```

**Parameters:**
- `--job-id` (required): Job ID to cancel
- `--reason` (optional): Cancellation reason (default: "Manual cancellation")

**Example:**
```bash
python pipeline_manager.py cancel-job --job-id job_20250102_143522 --reason "Incorrect parameters"
```

---

### 2. status

Check detailed status of a pipeline job.

**Usage:**
```bash
python pipeline_manager.py status --job-id <JOB_ID>
```

**Parameters:**
- `--job-id` (required): Job ID to check

**Example:**
```bash
python pipeline_manager.py status --job-id job_20250102_143522
```

**Status Values:**
- `running` - Job is currently executing
- `success` - Job completed successfully
- `failed` - Job failed with errors
- `cancelled` - Job was manually cancelled
- `NOT_FOUND` - Job ID not found in monitoring dataset

---

### 3. retry

Retry a failed job from the appropriate checkpoint.

**Usage:**
```bash
python pipeline_manager.py retry --job-id <JOB_ID> [--force]
```

**Parameters:**
- `--job-id` (required): Job ID to retry
- `--force` (optional): Execute the retry (without this, shows dry-run)

**Example:**
```bash
# Dry-run (shows what would happen)
python pipeline_manager.py retry --job-id job_20250102_143522

# Execute the retry
python pipeline_manager.py retry --job-id job_20250102_143522 --force
```

---

### 4. cleanup

Perform maintenance cleanup operations.

**Usage:**
```bash
python pipeline_manager.py cleanup [--force] [OPTIONS]
```

**Parameters:**
- `--force` (required for execution): Actually perform cleanup
- `--bucket-name` (optional): GCS bucket name for cleanup
- `--job-log-days` (optional): Job log retention days (default: 90)
- `--checkpoint-days` (optional): Checkpoint retention days (default: 30)
- `--ttl-days` (optional): TTL cleanup threshold days (default: 7)

**Example:**
```bash
# Dry-run (shows what would be cleaned)
python pipeline_manager.py cleanup

# Execute cleanup with custom retention
python pipeline_manager.py cleanup --force --job-log-days 60 --ttl-days 5
```

**What Gets Cleaned:**
1. **Job Logs**: Execution records in monitoring dataset
2. **Checkpoints**: Resume points for failed jobs
3. **Archived Tables**: BigQuery tables with `_archive_` prefix
4. **GCS Files**: Files in staging/archive paths

---

### 5. health

Check pipeline health status.

**Usage:**
```bash
python pipeline_manager.py health [--source <SOURCE>] [--hours-back <HOURS>]
```

**Parameters:**
- `--source` (optional): Filter by source (facebook, wordpress)
- `--hours-back` (optional): Hours to analyze (default: 24)

**Example:**
```bash
# All sources, last 24 hours
python pipeline_manager.py health

# Facebook only, last 48 hours
python pipeline_manager.py health --source facebook --hours-back 48
```

**Health Score Ranges:**
- 90-100: Excellent
- 80-89: Good
- 70-79: Fair
- 0-69: Poor

---

### 6. list-jobs

List recent pipeline jobs.

**Usage:**
```bash
python pipeline_manager.py list-jobs [--source <SOURCE>] [--table <TABLE>] [--limit <N>]
```

**Parameters:**
- `--source` (optional): Filter by source
- `--table` (optional): Filter by table name
- `--limit` (optional): Maximum jobs to list (default: 20)

**Example:**
```bash
# List last 10 jobs
python pipeline_manager.py list-jobs --limit 10

# Facebook campaigns only
python pipeline_manager.py list-jobs --source facebook --table campaigns
```

---

### 7. metrics

Get enhanced pipeline metrics including trends, quality, and insights.

**Usage:**
```bash
python pipeline_manager.py metrics [OPTIONS]
```

**Parameters:**
- `--source` (optional): Filter by source
- `--hours-back` (optional): Hours to analyze (default: 24)
- `--days-back` (optional): Days for trend analysis (default: 7)
- `--show-trends`: Show performance trends
- `--show-quality`: Show data quality metrics
- `--show-insights`: Show operational insights

**Example:**
```bash
# Basic metrics
python pipeline_manager.py metrics --hours-back 24

# Full analysis
python pipeline_manager.py metrics --hours-back 48 --show-trends --show-quality --show-insights
```

---

### 8. resources

Get detailed resource usage and cost analysis.

**Usage:**
```bash
python pipeline_manager.py resources [--source <SOURCE>] [--hours-back <HOURS>] [--show-cost-breakdown]
```

**Parameters:**
- `--source` (optional): Filter by source
- `--hours-back` (optional): Hours to analyze (default: 24)
- `--show-cost-breakdown`: Show detailed cost breakdown

**Example:**
```bash
# Basic resource analysis
python pipeline_manager.py resources --hours-back 24

# Detailed cost breakdown
python pipeline_manager.py resources --hours-back 48 --show-cost-breakdown
```

---

### 9. dependencies

Manage pipeline dependencies and conditional execution.

**Usage:**
```bash
python pipeline_manager.py dependencies --check-dependency [OPTIONS]
python pipeline_manager.py dependencies --wait-for <DEP1> <DEP2> ... [OPTIONS]
python pipeline_manager.py dependencies --conditional [OPTIONS]
```

**Example:**
```bash
# Check if table has sufficient data
python pipeline_manager.py dependencies \
  --check-dependency \
  --dep-name "staging_data" \
  --dep-type data_availability \
  --table-id "bi-data-391216.FACEBOOK_STAGING.campaigns" \
  --min-rows 100 \
  --max-age-hours 12
```

---

### 10. templates

Manage reusable pipeline templates.

**Usage:**
```bash
python pipeline_manager.py templates --create [OPTIONS]
python pipeline_manager.py templates --execute [OPTIONS]
python pipeline_manager.py templates --list
```

**Example:**
```bash
# List available templates
python pipeline_manager.py templates --list

# Execute a template
python pipeline_manager.py templates --execute --template-name "facebook_daily_full"
```

---

## Common Workflows

### Workflow 1: Daily Health Check

```bash
# 1. Check overall health
python pipeline_manager.py health --hours-back 24

# 2. List recent jobs
python pipeline_manager.py list-jobs --limit 20

# 3. Check resource usage
python pipeline_manager.py resources --hours-back 24
```

### Workflow 2: Job Failure Investigation

```bash
# 1. Check job status
python pipeline_manager.py status --job-id job_20251023_143522

# 2. View health metrics to see if it's a pattern
python pipeline_manager.py health --source facebook --hours-back 48

# 3. Retry the job
python pipeline_manager.py retry --job-id job_20251023_143522 --force
```

### Workflow 3: Weekly Maintenance

```bash
# 1. Check health over past week
python pipeline_manager.py health --hours-back 168

# 2. Review metrics and trends
python pipeline_manager.py metrics --days-back 7 --show-trends --show-quality --show-insights

# 3. Perform cleanup (dry-run first)
python pipeline_manager.py cleanup

# 4. Execute cleanup if safe
python pipeline_manager.py cleanup --force --job-log-days 90 --ttl-days 7
```

---

## Troubleshooting

### Issue: "Your default credentials were not found"

**Cause:** `.env` file is missing or GCP credentials are not configured properly.

**Solution:**
```bash
# 1. Verify .env file exists in orchestrator directory
ls -la .env

# 2. Check .env contains required variables
cat .env

# 3. Verify service account file exists
ls -la c:\gcp\service-account-bionews-pipeline.json

# 4. Test credentials with gcloud
gcloud auth application-default login
```

### Issue: "Job not found in monitoring dataset"

**Cause:** Job ID doesn't exist or monitoring dataset is incorrect.

**Solution:**
```bash
# List recent jobs to verify ID
python pipeline_manager.py list-jobs --limit 50

# Use correct monitoring dataset if non-default
python pipeline_manager.py status --job-id job_20251023_143522 --monitoring-dataset orchestrator_monitoring_prod
```

### Issue: Health score is low

**Cause:** Recent failures or performance degradation.

**Investigation:**
```bash
# 1. Check detailed metrics
python pipeline_manager.py metrics --hours-back 48 --show-insights

# 2. List recent failed jobs
python pipeline_manager.py list-jobs --limit 50

# 3. Check specific job failures
python pipeline_manager.py status --job-id <FAILED_JOB_ID>
```

---

## Advanced Usage

### Scheduled Health Checks (cron)

```bash
# Add to crontab for daily 8 AM health check
0 8 * * * cd /path/to/orchestrator && python pipeline_manager.py health --hours-back 24 > /var/log/pipeline_health.log 2>&1
```

### Automated Cleanup (cron)

```bash
# Weekly cleanup every Sunday at 2 AM
0 2 * * 0 cd /path/to/orchestrator && python pipeline_manager.py cleanup --force --job-log-days 90 --ttl-days 7 > /var/log/pipeline_cleanup.log 2>&1
```

### Dependency-Based Execution

```bash
# Wait for upstream pipeline, then execute
python pipeline_manager.py dependencies \
  --wait-for facebook_extraction \
  --dep-type pipeline_completion \
  --max-wait-minutes 60 && \
python orchestrate.py --source wordpress --env prod
```

---

## Exit Codes

- `0` - Success
- `1` - Error or operation failed
- `2` - Invalid arguments

All commands return appropriate exit codes for scripting.

---

## Machine Tracking & Job Lineage (v1.1.0)

### Overview

Pipeline Manager now tracks machine information for all jobs, enabling cross-platform monitoring and remote job management.

### Machine Information Tracked

Every job records:
- **hostname** - Machine name where job executed
- **ip_address** - IP address of the machine
- **os_type** - Operating system (Windows/Linux/Darwin)
- **os_version** - OS version string
- **python_version** - Python interpreter version
- **process_id** - OS process ID (PID)
- **parent_process_id** - Parent process PID
- **user** - Username who ran the job
- **working_directory** - Execution directory
- **is_remote_killable** - Whether job can be killed remotely

### Viewing Machine Information

```bash
# Show jobs with machine info
python pipeline_manager.py list-jobs --show-machine

# Filter by hostname
python pipeline_manager.py list-jobs --hostname rlm-bionews

# View specific job with full details
python pipeline_manager.py job-status --job-id abc-123-def-456
```

### Job Lineage Tracking

Jobs can now track parent-child relationships using `parent_job_id`:

**Query lineage in BigQuery**:
```sql
-- Find all child jobs
SELECT
  job_id,
  source,
  tables,
  status,
  hostname,
  created_at
FROM `orchestrator_monitoring.jobs`
WHERE parent_job_id = 'parent-job-uuid'
ORDER BY created_at

-- Job family tree
WITH RECURSIVE job_tree AS (
  SELECT job_id, parent_job_id, source, tables, hostname, 0 as level
  FROM `orchestrator_monitoring.jobs`
  WHERE job_id = 'root-job-id'

  UNION ALL

  SELECT j.job_id, j.parent_job_id, j.source, j.tables, j.hostname, jt.level + 1
  FROM `orchestrator_monitoring.jobs` j
  JOIN job_tree jt ON j.parent_job_id = jt.job_id
)
SELECT * FROM job_tree ORDER BY level, created_at
```

### Remote Job Management

**Kill jobs on specific machines** (requires SSH access):
```bash
# View running jobs on specific host
python pipeline_manager.py list-jobs --status running --hostname server-01

# Kill specific job (if on same machine)
python pipeline_manager.py cancel-job --job-id abc-123 --kill-process
```

**Note**: Remote killing across machines requires SSH/WinRM configuration.

### Orphaned Job Detection

Pipeline Manager automatically detects and marks orphaned jobs:

```bash
# List recent jobs (auto-detects orphaned jobs)
python pipeline_manager.py list-jobs

# Output shows:
# "Marked 5 orphaned jobs as KILLED on hostname-xyz"
```

Orphaned jobs are RUNNING status but process no longer exists.

---

## Related Documentation

- [README.md](../README.md) - Main orchestrator documentation
- [USER_MANUAL.md](USER_MANUAL.md) - User guide for orchestrate.py
- [JOB_CHAINING_GUIDE.md](../JOB_CHAINING_GUIDE.md) - Workflow automation
- [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md) - Technical implementation details
- [YAML_CONFIGURATION_MANUAL.md](YAML_CONFIGURATION_MANUAL.md) - Configuration file format

---

## Support

For issues or questions:
- Internal: Contact Data Pipeline Team
- Documentation: See `docs/` directory
- Logs: Check `orchestrator_monitoring.jobs` table in BigQuery

---

**End of Pipeline Manager User Manual - v1.1.0**
