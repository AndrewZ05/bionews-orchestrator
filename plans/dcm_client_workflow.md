# DCM Client Account Workflow

## Overview

This document explains how the DCM extractor handles **dynamic client account discovery** for agencies/service providers managing multiple client accounts.

---

## Architecture: API-Only, Dynamic Discovery

```
┌─────────────────────────────────────────────────────────────┐
│                    YOUR PIPELINE                             │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Service Account:                                       │ │
│  │ pipeline-workflow@bi-data-391216.iam.gserviceaccount  │ │
│  └────────────────────────────────────────────────────────┘ │
│                            │                                 │
│                            │ 1. Start ETL Job                │
│                            ▼                                 │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ get_available_sites()                                  │ │
│  │ • Calls: userprofiles().list()                         │ │
│  │ • Discovers ALL accessible profiles                    │ │
│  │ • Returns: [7560911, 9515, 636, ...]                  │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ 2. API Call
                            ▼
         ┌──────────────────────────────────────┐
         │   Campaign Manager 360 API            │
         │   (Google Cloud)                      │
         └──────────────────────────────────────┘
                            │
                            │ 3. Returns accessible profiles
                            ▼
┌────────────────────────────────────────────────────────────────┐
│                    CLIENT ACCOUNTS                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │ Profile 7560911│ │ Profile 9515 │  │ Profile 636   │        │
│  │ Client A       │ │ Client B     │  │ Client C      │  ...   │
│  │ ✓ Has Access   │ │ ✓ Has Access │  │ ✓ Has Access  │        │
│  └──────────────┘  └──────────────┘  └──────────────┘        │
└────────────────────────────────────────────────────────────────┘
                            │
                            │ 4. For each profile: create + run report
                            ▼
         ┌──────────────────────────────────────┐
         │   Extract Campaign Performance Data   │
         │   • Create report                     │
         │   • Run report (async)                │
         │   • Wait for completion               │
         │   • Download CSV                      │
         │   • Load to BigQuery                  │
         └──────────────────────────────────────┘
```

---

## Daily ETL Job Flow

### **Step 1: Dynamic Account Discovery** (Every Run)

```python
# Called automatically by orchestrator at job start
profiles = get_available_sites(config)

# API Call:
# GET https://dfareporting.googleapis.com/dfareporting/v4/userprofiles
# Auth: Bearer <service_account_token>

# Returns:
# {
#   "items": [
#     {"profileId": 7560911, "accountName": "Client A", ...},
#     {"profileId": 9515, "accountName": "Client B", ...},
#     ...
#   ]
# }

# Result: [7560911, 9515, 636, ...]
```

**What this means:**
- ✅ New client adds your service account → automatically included
- ✅ Client removes your access → automatically excluded
- ✅ No config changes needed
- ✅ Audit trail saved to `dcm_discovered_accounts.json`

---

### **Step 2: Report Generation** (Per Profile)

For each discovered profile:

```python
for profile_id in profiles:
    # 1. Create report
    report_config = {
        "name": f"Campaign_Performance_{date}",
        "type": "STANDARD",
        "criteria": {
            "dateRange": {
                "startDate": "2024-11-10",  # 7-day lookback
                "endDate": "2024-11-17"
            },
            "dimensions": ["date", "campaign", "placement"],
            "metricNames": ["impressions", "clicks", "conversions"]
        }
    }

    report_id = client.create_report(profile_id, report_config)

    # 2. Run report (asynchronous)
    file_id = client.run_report(profile_id, report_id)

    # 3. Wait for completion (30 sec polling)
    # Status: QUEUED → PROCESSING → REPORT_AVAILABLE
    download_url = client.wait_for_report(profile_id, report_id, file_id)

    # 4. Download CSV
    client.download_report(profile_id, report_id, file_id, output_path)

    # 5. Load to BigQuery
    load_to_bigquery(output_path, table_name)
```

**Timeline per profile:**
- Report creation: ~1 second
- Report processing: ~5-30 minutes (varies by data volume)
- Download: ~10-30 seconds
- BigQuery load: ~30 seconds

**For 75 profiles:**
- Sequential: ~6-38 hours (not feasible!)
- Parallel (5 workers): ~1-8 hours ✅
- Most time is waiting for Google to generate reports

---

### **Step 3: Data Merge** (Incremental with Lookback)

```sql
-- Incremental load with 7-day lookback
-- Pulls: 2024-11-10 to 2024-11-17

-- BigQuery MERGE handles:
-- 1. New records (inserts)
-- 2. Updated records (updates) - attribution changes
-- 3. Primary key: (date, campaign_id, placement_id, ad_id)

MERGE `project.dcm_data.dcm_campaign_performance` AS target
USING `staging_table` AS source
ON target.date = source.date
   AND target.campaign_id = source.campaign_id
   AND target.placement_id = source.placement_id
   AND target.ad_id = source.ad_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT ROW;
```

---

## Client Onboarding Workflow

### **New Client Setup:**

```
Day 0: Client signs contract
  ↓
Day 1: Client grants access in DCM
  • Client logs into Campaign Manager 360
  • Admin → User Profiles
  • Adds: pipeline-workflow@bi-data-391216.iam.gserviceaccount.com
  • Grants: "Reporting" permission
  ↓
Day 2: Next scheduled ETL job runs (9am)
  • get_available_sites() API call
  • Discovers new profile automatically
  • Extracts data
  • Loads to BigQuery
  ↓
✅ Client data flowing - ZERO CODE CHANGES
```

### **Client Offboarding:**

```
Day 1: Client cancels/removes access
  • Client removes service account from DCM
  ↓
Day 2: Next ETL job runs
  • get_available_sites() returns N-1 profiles
  • Removed client NOT processed
  • No errors, no failed jobs
  ↓
✅ Clean automatic removal
```

---

## Why DTS is NOT an Option

| Feature | DTS (Data Transfer Service) | Reporting API |
|---------|----------------------------|---------------|
| **Account ownership** | Must OWN the account | Read-only access OK ✅ |
| **Setup** | Requires Google enablement + fees | Just API access ✅ |
| **GCS bucket** | Requires your bucket with permissions | Not needed ✅ |
| **Data granularity** | Event-level (impression_id) | Aggregated only |
| **Historical data** | 60 days only | 2-3 years ✅ |
| **Client accounts** | ❌ Not available | ✅ Works perfectly |
| **Cost** | Setup fee + ongoing | Free ✅ |

**For agency/client model → Reporting API is the ONLY option**

---

## Audit Trail

Every job creates `dcm_discovered_accounts.json`:

```json
{
  "discovered_at": "2024-11-18T09:00:00",
  "count": 75,
  "profiles": [
    {
      "profile_id": 7560911,
      "account_id": 123456,
      "account_name": "Client A - Campaign Manager",
      "user_name": "adoperations.bionews"
    },
    {
      "profile_id": 9515,
      "account_id": 789012,
      "account_name": "Client B - DCM",
      "user_name": "adops_client_b"
    }
  ]
}
```

**Use this to:**
- Track client account additions/removals
- Audit who has granted access
- Troubleshoot missing clients
- Report to stakeholders

---

## Monitoring

### **Key Metrics to Track:**

```yaml
# Number of accessible clients
SELECT COUNT(DISTINCT _profile_id) as client_count
FROM dcm_data.dcm_campaign_performance
WHERE DATE(_extracted_at) = CURRENT_DATE()

# Client data freshness
SELECT
  _profile_id,
  _account_name,
  MAX(date) as latest_data_date,
  MAX(_extracted_at) as last_extraction
FROM dcm_data.dcm_campaign_performance
GROUP BY 1, 2
ORDER BY 3 DESC

# New clients this week
SELECT profile_id, account_name
FROM dcm_discovered_accounts_history
WHERE discovered_at > CURRENT_DATE() - 7
  AND profile_id NOT IN (
    SELECT profile_id
    FROM dcm_discovered_accounts_history
    WHERE discovered_at < CURRENT_DATE() - 7
  )
```

---

## Benefits of Dynamic Discovery

✅ **No manual config updates** - New clients automatically included
✅ **Scalable** - Handle 75, 100, 200+ clients with same code
✅ **Self-documenting** - Audit trail of accessible accounts
✅ **Resilient** - Removed clients don't cause failures
✅ **Client-driven** - Clients control their own access
✅ **No maintenance** - Set it and forget it

---

## Comparison: Static vs Dynamic

### **OLD WAY (Static Config):**

```yaml
# configs/dcm.yaml
accounts: [7560911, 9515, 636, ...]  # Hardcoded list

# Problems:
# ❌ New client = code change + deployment
# ❌ Removed client = still in config, causes errors
# ❌ Out of sync with reality
# ❌ Manual maintenance burden
```

### **NEW WAY (Dynamic Discovery):**

```yaml
# configs/dcm.yaml
accounts: all  # Discover at runtime

# Benefits:
# ✅ New client = automatically included next run
# ✅ Removed client = automatically excluded
# ✅ Always in sync with DCM permissions
# ✅ Zero maintenance
```

---

## Best Practices

### **1. Run Daily (Recommended)**

```bash
# Cron: 9am daily
0 9 * * * python orchestrate.py --source dcm --env prod --group campaign_performance
```

**Why daily?**
- Captures new clients within 24 hours
- 7-day lookback handles retroactive attribution
- Balances freshness vs API quota usage

### **2. Monitor Discovered Accounts**

```bash
# Check for changes
diff dcm_discovered_accounts.json dcm_discovered_accounts_prev.json

# Alert on significant changes
NEW_COUNT=$(jq '.count' dcm_discovered_accounts.json)
if [ $NEW_COUNT -ne 75 ]; then
  echo "Client count changed: was 75, now $NEW_COUNT"
  # Send alert
fi
```

### **3. Save Discovery History**

```bash
# After each run, archive discovery file
cp dcm_discovered_accounts.json \
   dcm_discovered_accounts_$(date +%Y%m%d).json

# Keep 30 days of history
find . -name "dcm_discovered_accounts_*.json" -mtime +30 -delete
```

### **4. Test New Clients**

When notified of new client:

```bash
# Get their profile ID from discovery file
NEW_PROFILE_ID=$(jq -r '.profiles[] | select(.account_name | contains("New Client")) | .profile_id' dcm_discovered_accounts.json)

# Test extraction for just that client
python orchestrate.py --source dcm --env prod --group campaign_performance --accounts $NEW_PROFILE_ID --test-mode
```

---

## Troubleshooting

### **"Discovered 0 profiles"**

**Cause:** Service account has no access to any DCM accounts

**Fix:**
1. Check service account email is correct
2. Have clients add service account to their DCM profiles
3. Wait 5-10 minutes for permissions to propagate

### **"Discovered fewer profiles than expected"**

**Cause:** Some clients removed access or permissions expired

**Fix:**
1. Check `dcm_discovered_accounts.json` to see who's missing
2. Contact missing clients to verify access
3. Have them re-add service account if needed

### **"Report failed for profile X"**

**Cause:** Client's account has issues (suspended, no data, etc.)

**Fix:**
1. Check client account status in DCM UI
2. Verify date range has data
3. Skip this client temporarily:
   ```yaml
   # configs/dcm.yaml
   accounts: [7560911, 9515]  # Exclude problematic profile temporarily
   ```

---

## Summary

**Key Takeaways:**

1. ✅ **API-only approach** - No DTS needed or available
2. ✅ **Dynamic discovery** - New clients automatically included
3. ✅ **Zero maintenance** - No config updates for new/removed clients
4. ✅ **Scalable** - Handle unlimited client accounts
5. ✅ **Audit trail** - Track client access over time
6. ✅ **Resilient** - Gracefully handles client changes

**This is the RIGHT architecture for agency/client account management.**
