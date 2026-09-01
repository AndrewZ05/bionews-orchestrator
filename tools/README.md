# Orchestrator Tools

Utility scripts for maintenance, debugging, and verification tasks.

## Available Tools

### job_inspector.py

CLI tool for monitoring and inspecting orchestrator jobs.

**Usage:**

```bash
# View recent jobs
python tools/job_inspector.py --recent 10

# Inspect specific job
python tools/job_inspector.py --job-id <job_id>

# Query by source
python tools/job_inspector.py --source facebook --recent 5

# Check job status
python tools/job_inspector.py --status FAILED --recent 20
```

**Features:**
- View job history and details
- Check job status and duration
- See table processing statistics
- Filter by source, environment, or status
- Environment-specific queries (--env prod/dev/test)

---

### validate_config_extractor_alignment.py

Validates that extractor code aligns with YAML configuration schemas.

**Usage:**

```bash
# Validate all sources
python tools/validate_config_extractor_alignment.py

# Validate specific source
python tools/validate_config_extractor_alignment.py --source facebook
```

**What it checks:**
- YAML config completeness
- Extractor code matches configured tables
- Schema field alignment
- Missing or extra configurations

**When to use:**
- After updating YAML configs
- Before deploying extractor changes
- When adding new tables or fields
- Troubleshooting extraction issues

---

## Archived Tools

### fix_facebook_hashes.py (REMOVED - October 2025)

This tool was used for a one-time hash migration from MD5 to FARM_FINGERPRINT. The migration is complete and the underlying issue is permanently fixed in the codebase.

**Usage:**

```bash
# Preview changes (no modifications)
python tools/fix_facebook_hashes.py --dry-run

# Preview specific tables
python tools/fix_facebook_hashes.py --dry-run --tables campaigns,adsets

# Verify hash consistency
python tools/fix_facebook_hashes.py --verify

# Apply hash updates
python tools/fix_facebook_hashes.py --apply

# Apply to specific tables
python tools/fix_facebook_hashes.py --apply --tables campaigns
```

**Options:**

- `--dry-run` - Preview SQL without executing (safe mode)
- `--apply` - Execute hash updates
- `--verify` - Check hash consistency against computed FARM_FINGERPRINT
- `--tables` - Comma-separated list of tables (default: all active tables)
- `--project` - GCP project ID (default: bi-data-391216)
- `--dataset` - BigQuery dataset (default: facebook_data)

**When to Use:**

- After reloading Facebook data from backup
- When migrating to new environment
- After schema changes requiring hash regeneration
- To verify hash consistency after maintenance
- When troubleshooting incremental update issues

**Safety Features:**

- Dry-run mode shows exactly what will happen
- Only updates rows with NULL or incorrect hashes
- Configuration-driven (reads schema from facebook.yaml)
- Idempotent (safe to rerun)
- Detailed progress reporting

**Example Output:**

```
================================================================================
Facebook Hash Migration Tool
================================================================================
Mode: VERIFY
Project: bi-data-391216
Dataset: facebook_data
================================================================================

[campaigns] facebook_campaigns
--------------------------------------------------------------------------------
  Current state:
    Total rows: 876
    NULL hashes: 0
    Existing hashes: 876
    Table size: 0.19 MB
  Verifying hash consistency...
    Correct hashes: 876 (100.0%)
    Incorrect hashes: 0
    NULL hashes: 0
  [OK] All hashes are correct

================================================================================
SUMMARY
================================================================================
Verified: 15
Needs update: 0
Errors: 0
================================================================================
```

## Adding New Tools

When creating new tools:

1. Place in `tools/` directory
2. Add shebang: `#!/usr/bin/env python3`
3. Include docstring explaining purpose and usage
4. Add to this README with usage examples
5. Follow project coding standards (function-based, no classes, no emojis)
6. Include `dotenv.load_dotenv()` if BigQuery access needed
7. Use `shared/bigquery_client.py` for consistent credentials

## Related Documentation

- [REFACTORING_COMPLETE.md](../REFACTORING_COMPLETE.md) - Hash migration background
- [TABLE_TRACKING_INTEGRATION.md](../TABLE_TRACKING_INTEGRATION.md) - Table tracking system
- [configs/facebook.yaml](../configs/facebook.yaml) - Facebook table schemas
