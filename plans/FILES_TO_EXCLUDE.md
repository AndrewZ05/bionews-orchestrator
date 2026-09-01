# Files That Should NOT Be Stored in Git

This document lists files and directories that should be excluded from version control.

## [OK] Already Properly Excluded (in .gitignore)

### Python Runtime Files
- `__pycache__/` directories
- `*.pyc`, `*.pyo`, `*.py[cod]` compiled bytecode
- `*$py.class` class files
- `*.so` shared object files
- Virtual environments: `env/`, `venv/`, `ENV/`

### Build & Distribution
- `build/`, `dist/`, `downloads/`
- `eggs/`, `.eggs/`, `*.egg-info/`
- `sdist/`, `develop-eggs/`
- `lib/`, `lib64/`, `Lib/`, `parts/`, `var/`

### Logs & Temporary Files
- **`logs/`** - Pipeline execution logs (uploaded to GCS)
- **`recovery/`** - Checkpoint and recovery data
- **`temp/`** - Temporary processing files
- `*.log` files

### Data Files
- **`data/`** directory - Local data cache
- `*.parquet` - Parquet data files
- `*.csv`, `*.tsv` - CSV/TSV exports
- `*.jsonl` - JSON Lines files
- `*.duckdb` - DuckDB database files
- `facebook_staging.duckdb` - Specific DB file

### Configuration & Secrets
- **`.env`** - Environment variables (contains secrets!)

### IDE & OS Files
- `.idea/` - IntelliJ IDEA
- `.vscode/` - Visual Studio Code settings
- `.DS_Store` - macOS Finder metadata
- `Thumbs.db`, `desktop.ini` - Windows metadata
- `*.swp`, `*.swo`, `*~` - Vim swap files

### Backup Files
- **`configs/backups/`** - Auto-generated YAML backups
- `*.backup` - Manual backup files
- `*.backup_*` - Timestamped backups
- `*.CORRUPTED` - Corrupted file markers

### Session Documentation (NOT for version control)
These are temporary analysis/debug documents created during development sessions:

- `*_COMPLETE.md` - Session completion notes
- `*_FIX.md` - Bug fix documentation
- `*_ANALYSIS.md` - Code analysis notes
- `*_SUMMARY.md` - Session summaries
- `*_FINDINGS.md` - Investigation results
- `*_PLAN.md` - Implementation plans
- `*_REVIEW.md` - Code review notes
- `*_SESSION*.md` - Session-specific docs
- `*_REQUIREMENTS*.md` - Requirement specs
- `*_IMPLEMENTATION*.md` - Implementation details
- `*_MIGRATION*.md` - Migration notes
- `ARCHITECTURAL_*.md` - Architecture analysis
- `FACEBOOK_*.md` - Facebook-specific session docs
- `INSIGHTS_*.md` - Insights analysis docs
- `MULTI_TENANT_*.md` - Multi-tenancy notes
- `PAGES_*.md` - Pages feature docs
- `EXTRACTOR_*.md` - Extractor development docs
- `PHASE*.md`, `PHASE*.txt` - Phase documentation
- `SESSION_*.md` - Generic session docs

### Test & Debug Scripts (NOT for version control)
One-time use scripts (keep only `run_tests.py`):

- `test_*.py` - Ad-hoc test scripts
- `add_*.py` - One-time data addition scripts
- `fix_*.py` - Bug fix scripts
- `check_*.py` - Validation check scripts
- `compare_*.py` - Comparison scripts
- `debug_*.py` - Debug helper scripts
- `discover_*.py` - Discovery scripts
- `execute_*.py` - Execution scripts
- `monitor_*.py` - Monitoring scripts
- `validate_*.py` - Validation scripts
- `verify_*.py` - Verification scripts
- `update_*.py` - Update scripts
- `create_*.py` - Creation scripts
- `audit_*.py` - Audit scripts
- `merge_*.py` - Merge scripts
- `manual_*.py` - Manual operation scripts
- `delete_*.py` - Deletion scripts
- `count_*.py` - Counting scripts
- `generate_*_schemas.py` - Schema generation (except `generate_yaml_schemas.py`)

### Reference Files (NOT for version control)
- `*.txt` files (except `requirements.txt` and `Linux set up.txt`)
- `WORDPRESS_SCHEMAS_TO_ADD.yaml`
- `facebook_all_fields_reference.*`
- `facebook_pages.*`
- `insights_analysis.txt`
- `pages_posts_fields_discovery.txt`
- `CROSS_EXTRACTOR_ANALYSIS.txt`
- `OLD_CODE_AUDIT.txt`
- `LEGACY_CLEANUP_COMPLETE.txt`

### Test Output Files
- `test_results/*.json`
- `test_results/*.tsv`
- `*.json` files (except `configs/*.json`)
- `fb_api_*.json`, `fb_api_*.tsv`
- `test_report_*.json`
- `*.output`
- `valid_metrics.log`
- `wordpress_*.log`, `facebook_*.log`
- `account_insights_*.log`
- `page_engagement_*.log`
- `gf_*.log`

### Shell Scripts (auto-generated for testing)
- `test_single_page.bat`, `test_single_page.sh`
- `wordpress_add_site_id.sh`
- `RUN_ALL_TESTS.bat`

### SQL Files (one-time migrations)
- `facebook_migration*.sql`
- `wordpress_migration*.sql`
- `rename_tables.sql`
- `extract_production_schemas.sql`
- `wordpress_schema_cleanup.sql`

### Documentation (NOT for version control)
- `*.docx` - Word documents
- `~$*.docx` - Word temp files

### DLT Framework (removed from project)
- `.dlt/` - DLT configuration and state

### Schema Files (use YAML configs instead)
- `schemas/*.yaml` - Old schema directory

---

## [OK] Essential Files That SHOULD Be in Git

### Core Application Files
- `orchestrate.py` - Main CLI entry point [OK]
- `run_tests.py` - Test runner [OK]
- `requirements.txt` - Python dependencies [OK]
- `env.example` - Environment template (no secrets) [OK]
- `.gitignore` - Git ignore rules [OK]
- `README.md` - Main documentation [OK]

### Essential Documentation
- `DATA_FLOW_MANUAL.md` - Data flow guide [OK]
- `USER_MANUAL.md` - User guide [OK]
- `IMPLEMENTATION_GUIDE.md` - Developer guide [OK]
- `YAML_CONFIGURATION_MANUAL.md` - Config reference [OK]

### Configuration Files
- `configs/facebook.yaml` - Facebook config [OK]
- `configs/wordpress.yaml` - WordPress config [OK]
- `configs/defaults.yaml` - Default settings [OK]
- `configs/alerting.yaml` - Alert config [OK]
- `configs/templates/` - Config templates [OK]

### Source Plugins
- `plugins/facebook_extractor.py` [OK]
- `plugins/wordpress_extractor.py` [OK]
- `plugins/generic_extractor.py` [OK]

### Shared Utilities
All files in `shared/`:
- `bigquery_client.py` [OK]
- `bigquery_utils.py` [OK]
- `gcs_storage.py` [OK]
- `gcs_pipeline.py` [OK]
- `external_tables.py` [OK]
- `transform.py` [OK]
- `schema_discovery.py` [OK]
- `data_validator.py` [OK]
- `config_loader.py` [OK]
- `error_handler.py` [OK]
- `rate_limiter.py` [OK]
- `monitoring.py` [OK]
- `notifications.py` [OK]
- `pipeline_utils.py` [OK]
- `pipeline_validator.py` [OK]
- And other shared modules...

---

## [SEARCH] How to Check What's Excluded

```bash
# See what's currently tracked
git status

# See what would be committed
git add . --dry-run

# Check if a specific file is ignored
git check-ignore -v <filename>

# List all untracked files (including ignored)
git status --ignored
```

---

## 🧹 Cleanup Commands

```bash
# Remove Python cache directories
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
find . -name "*.pyc" -delete
find . -name "*.pyo" -delete

# Remove log files
rm -rf logs/*
rm -f *.log

# Remove data files
rm -rf data/*
rm -f *.parquet *.csv *.jsonl

# Remove session documentation
rm -f *_COMPLETE.md *_FIX.md *_ANALYSIS.md *_SUMMARY.md
rm -f ARCHITECTURAL_*.md FACEBOOK_*.md INSIGHTS_*.md
rm -f MULTI_TENANT_*.md PAGES_*.md EXTRACTOR_*.md

# Remove test scripts
rm -f test_*.py fix_*.py check_*.py
# (Keep run_tests.py!)

# Remove backup files
rm -f *.backup* *.CORRUPTED
rm -rf configs/backups/*
```

---

## [OK] Current Status

After running the cleanup and with the updated `.gitignore`:

### Files to Delete (not in git)
Run these commands to clean up:

```bash
# Clean Python cache
rm -rf plugins/__pycache__ shared/__pycache__ __pycache__

# Clean session docs (examples - check if they exist first)
# rm -f ARCHITECTURAL_*.md FACEBOOK_*.md INSIGHTS_*.md
# rm -f MULTI_TENANT_*.md PAGES_*.md EXTRACTOR_*.md
```

### Files to Add to Git
These should be committed:

```bash
git add configs/facebook.yaml
git add configs/wordpress.yaml
git add configs/alerting.yaml
git add configs/defaults.yaml
git add configs/templates/
git add shared/*.py
git add plugins/*.py
git add orchestrate.py
git add .gitignore
git add README.md
git add DATA_FLOW_MANUAL.md
git add USER_MANUAL.md
git add IMPLEMENTATION_GUIDE.md
git add YAML_CONFIGURATION_MANUAL.md
git commit -m "chore: Update repository with latest changes and documentation"
```

---

## [NOTE] Notes

1. **Logs are uploaded to GCS** - No need to store locally in git
2. **Data files are ephemeral** - They're processed and uploaded to BigQuery/GCS
3. **Session docs are temporary** - Keep only the 4 essential MD files
4. **Backup files are auto-generated** - The system creates them automatically
5. **`.env` contains secrets** - NEVER commit this file!

---

**Last Updated**: 2025-10-23
