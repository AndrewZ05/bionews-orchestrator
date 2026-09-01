# Smart Retry System - User Guide

## Overview

The smart retry system automatically discovers and retries failed extractions across **all data sources** (Mailchimp, WordPress, Facebook, LimeSurvey). No manual manifest tracking needed!

## Quick Start

### Simple Retry Commands

```bash
# Retry ALL failures for a source (auto-discovers all pending manifests)
python orchestrate.py --source mailchimp --env prod --retry

# Retry failures for specific group
python orchestrate.py --source mailchimp --env prod --group campaign_group --retry

# Retry failures for specific table
python orchestrate.py --source mailchimp --env prod --table campaign_locations --retry

# Works for any source
python orchestrate.py --source wordpress --env prod --retry
python orchestrate.py --source facebook --env prod --retry
python orchestrate.py --source limesurvey --env prod --retry
```

### Auto-Cleanup

After successful retry:
- **100% success**: Manifest automatically deleted ✓
- **Partial success**: New manifest created with remaining failures, original archived
- **No action needed**: System manages manifests for you

## How It Works

### 1. Normal Extraction (Failures Occur)

```bash
python orchestrate.py --source mailchimp --env prod --group campaign_group \
  --start-date 2025-06-01 --end-date 2025-06-30
```

**Output**:
```
================================================================================
EXTRACTION COMPLETED WITH PARTIAL FAILURES
================================================================================

Total failures: 8
Affected tables: 2

  campaign_locations: 6 items failed
    - 44d77a74f9
    - 779dd865e6
    ... and 4 more

  campaign_email_activity: 2 items failed
    - xyz123
    - abc456

Failure manifest saved to:
  failures/mailchimp_20251116_160741_abc123.json

To retry failed items:
  python orchestrate.py --source mailchimp --env prod --retry
================================================================================
```

### 2. Smart Retry (Auto-Discovery)

```bash
# Just specify source - system finds all failures
python orchestrate.py --source mailchimp --env prod --retry
```

**What happens**:
1. **Searches** `failures/` directory for mailchimp manifests
2. **Combines** failures from all matching manifests
3. **Extracts** only failed items (skips successful ones)
4. **Updates** manifests:
   - **If 100% success**: Deletes manifest
   - **If partial**: Creates new manifest, archives original

**Output**:
```
================================================================================
SMART RETRY MODE
================================================================================

Found 2 failure manifests for mailchimp:
  failures/mailchimp_20251116_160741_abc123.json (8 failures)
  failures/mailchimp_20251115_143022_xyz789.json (3 failures)

Combined failures: 11 items across 3 tables
  - campaign_locations: 6 items
  - campaign_email_activity: 3 items
  - campaign_click_details: 2 items

Starting retry extraction...

[Extraction runs]

================================================================================
RETRY RESULTS
================================================================================

Original failures: 11
Successful retries: 9 (81.8%)
Still failing: 2 (18.2%)

Manifest updates:
  ✓ failures/mailchimp_20251116_160741_abc123.json → DELETED (100% success)
  → failures/mailchimp_20251116_182530_retry1_xyz789.json (2 failures remain)

Archived:
  failures/archive/mailchimp_20251115_143022_xyz789_retry1_partial_20251116_182530.json

To retry remaining failures:
  python orchestrate.py --source mailchimp --env prod --retry
================================================================================
```

### 3. Filtered Retry (Table/Group Specific)

```bash
# Retry only campaign_locations failures
python orchestrate.py --source mailchimp --env prod --table campaign_locations --retry
```

**What happens**:
1. **Searches** for manifests with campaign_locations failures
2. **Extracts** only campaign_locations failed items
3. **Updates** only campaign_locations in manifests
4. **Leaves** other table failures for later

## Directory Structure

```
failures/
├── mailchimp_20251116_160741_abc123.json          # Active manifest
├── mailchimp_20251116_182530_retry1_xyz789.json   # After retry (partial success)
│
└── archive/                                        # Auto-archived manifests
    ├── mailchimp_20251116_160741_abc123_completed_20251116_182530.json
    └── mailchimp_20251115_143022_xyz789_retry1_partial_20251116_182530.json
```

## Manifest Lifecycle

```
Initial Extraction
    ↓
failures/mailchimp_20251116_160741_abc123.json
    ↓ (retry)
    ├─→ 100% success → DELETED ✓
    │
    └─→ Partial success
        ↓
        failures/mailchimp_20251116_182530_retry1_abc123.json (remaining failures)
        failures/archive/mailchimp_20251116_160741_abc123_retry1_partial_20251116_182530.json
        ↓ (retry again)
        ├─→ 100% success → DELETED ✓
        │
        └─→ Still failing
            ↓
            failures/mailchimp_20251116_193045_retry2_abc123.json
            failures/archive/mailchimp_20251116_182530_retry1_abc123_retry2_partial_20251116_193045.json
```

## Use Cases

### Use Case 1: Retry All Mailchimp Failures

```bash
# Scenario: Multiple extractions ran over the week, some had failures

# Simple command
python orchestrate.py --source mailchimp --env prod --retry

# System automatically:
# - Finds all mailchimp manifests
# - Combines all unique failures
# - Retries everything
# - Cleans up successful manifests
```

### Use Case 2: Focus on One Table

```bash
# Scenario: campaign_locations always has issues, want to retry just that

python orchestrate.py --source mailchimp --env prod --table campaign_locations --retry

# System:
# - Finds manifests with campaign_locations failures
# - Retries only campaign_locations
# - Updates manifests (other tables unchanged)
```

### Use Case 3: Retry Specific Group After Fix

```bash
# Scenario: Fixed API rate limiting issue, retry campaign_group

python orchestrate.py --source mailchimp --env prod --group campaign_group --retry

# System:
# - Finds manifests from campaign_group extractions
# - Retries all tables in that group
# - Clean slate after success
```

### Use Case 4: Cross-Source Retry

```bash
# Retry all sources with failures

for source in mailchimp wordpress facebook limesurvey; do
  python orchestrate.py --source $source --env prod --retry
done

# Or individually as needed
python orchestrate.py --source wordpress --env prod --retry
python orchestrate.py --source facebook --env prod --retry
```

## Advanced Features

### Manifest Priority

When multiple manifests exist, system processes newest first:
```
failures/mailchimp_20251116_160741_abc123.json  ← Processed first (newest)
failures/mailchimp_20251115_143022_xyz789.json  ← Processed second
failures/mailchimp_20251114_093015_def456.json  ← Processed third
```

### Deduplication

If same item fails in multiple manifests, it's retried only once:
```
Manifest 1: campaign_locations → [campaign_A, campaign_B]
Manifest 2: campaign_locations → [campaign_B, campaign_C]

Combined: campaign_locations → [campaign_A, campaign_B, campaign_C] (unique)
```

### Archive Retention

Archives are kept indefinitely for audit purposes:
- Review failure history
- Analyze error patterns
- Troubleshoot persistent issues

**Manual cleanup** (optional):
```bash
# Delete archives older than 30 days
find failures/archive -name "*.json" -mtime +30 -delete
```

## Monitoring

### Check Pending Retries

```bash
# List all active manifests
ls -lh failures/*.json

# Count total failures by source
for source in mailchimp wordpress facebook limesurvey; do
  count=$(jq -r '.summary.total_failures' failures/${source}_*.json 2>/dev/null | awk '{s+=$1} END {print s}')
  echo "$source: $count failures"
done
```

### View Manifest Details

```bash
# Pretty-print manifest
cat failures/mailchimp_20251116_160741_abc123.json | jq '.'

# Get summary
cat failures/mailchimp_20251116_160741_abc123.json | jq '.summary'

# List failed tables
cat failures/mailchimp_20251116_160741_abc123.json | jq -r '.summary.tables[]'

# Get failed item IDs for specific table
cat failures/mailchimp_20251116_160741_abc123.json | \
  jq -r '.failures_by_table.campaign_locations.failed_items[].item_id'
```

## Best Practices

### 1. Wait Before Retry

If seeing 503 errors, wait for API to recover:
```bash
# Wait 5 minutes
sleep 300

# Then retry
python orchestrate.py --source mailchimp --env prod --retry
```

### 2. Incremental Retry

Start with most critical tables:
```bash
# Priority 1: Core tables
python orchestrate.py --source mailchimp --env prod --table members --retry

# Priority 2: Campaign reports
python orchestrate.py --source mailchimp --env prod --group campaign_group --retry

# Priority 3: Everything else
python orchestrate.py --source mailchimp --env prod --retry
```

### 3. Verify Success

After retry, check BigQuery:
```sql
-- Check for missing campaign_locations data
SELECT
  c.id as campaign_id,
  c.emails_sent,
  COUNT(l.campaign_id) as location_count
FROM mailchimp_data.campaigns c
LEFT JOIN mailchimp_data.campaign_locations l ON c.id = l.campaign_id
WHERE c.send_time BETWEEN '2025-06-01' AND '2025-06-30'
GROUP BY c.id, c.emails_sent
HAVING location_count = 0
  AND c.emails_sent > 0
```

### 4. Scheduled Retry

Add to cron for automatic retry:
```bash
# Every night at 2 AM, retry all failures
0 2 * * * cd /path/to/orchestrator && python orchestrate.py --source mailchimp --env prod --retry >> logs/retry_$(date +\%Y\%m\%d).log 2>&1
```

## Troubleshooting

### Q: No manifests found

**A**: Check that failures directory exists and has .json files:
```bash
ls -la failures/
```

If empty, no failures occurred (or manifests were deleted).

### Q: Same items keep failing

**A**: Indicates persistent API issue:
1. Check Mailchimp API status
2. Review error messages in manifest
3. Increase retry delay
4. Reduce parallel workers further

### Q: Want to force re-extract (ignore manifest)

**A**: Temporarily move manifests out of way:
```bash
# Backup manifests
mkdir -p failures/backup
mv failures/*.json failures/backup/

# Run fresh extraction
python orchestrate.py --source mailchimp --env prod --group campaign_group \
  --start-date 2025-06-01 --end-date 2025-06-30

# Restore manifests if needed
mv failures/backup/*.json failures/
```

### Q: Manifest corrupted or invalid

**A**: System skips invalid manifests with warning. Delete or fix manually:
```bash
# Validate JSON
cat failures/mailchimp_20251116_160741_abc123.json | jq empty

# If invalid, delete
rm failures/mailchimp_20251116_160741_abc123.json
```

## Comparison: Old vs New

### Old Way (Manual)
```bash
# Find failures in logs
grep "Failed to fetch" logs/mailchimp_20251116.log > failed_campaigns.txt

# Manually identify campaign IDs
# ... manual parsing ...

# Create custom extraction query
# ... complex BigQuery logic ...

# Re-extract specific campaigns
# ... custom script ...

# Verify completeness
# ... more manual queries ...
```

### New Way (Automatic)
```bash
# One command
python orchestrate.py --source mailchimp --env prod --retry

# System handles everything:
# ✓ Finds failures
# ✓ Retries them
# ✓ Updates manifests
# ✓ Cleans up successes
```

## Related Documentation

- [FAILURE_TRACKING_AND_RETRY.md](FAILURE_TRACKING_AND_RETRY.md) - Technical implementation details
- [MAILCHIMP_EXTRACTION_GUIDE.md](MAILCHIMP_EXTRACTION_GUIDE.md) - Extraction commands
- [shared/failure_tracker.py](shared/failure_tracker.py) - Source code

---

**Version**: 1.0
**Last Updated**: 2025-11-16
**Status**: Design Complete (implementation pending)
