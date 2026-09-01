# CI/CD Testing Guide

## Overview

The `run_tests.py` script provides comprehensive automated testing for the Data Extraction Orchestrator. It's designed to run after every commit/push to validate code quality, configuration, and pipeline functionality.

## Quick Start

### Run CI/CD Tests (Recommended)
```bash
# Run fast critical tests (default)
python run_tests.py

# Or explicitly specify CI/CD mode
python run_tests.py --ci-mode
```

### Run All Tests
```bash
# Run comprehensive test suite
python run_tests.py --category all

# Run with verbose output
python run_tests.py --category all --verbose
```

## Test Categories

### 1. Syntax & Import Tests (SYN)
**What it tests:**
- Python syntax validation for all modules
- Import statements work correctly
- No compilation errors

**Tests included:**
- SYN-1.1 to SYN-1.10: Compile and import all Python modules

**When to run:** After every code change

```bash
python run_tests.py --category syntax
```

### 2. Environment Tests (ENV)
**What it tests:**
- Required environment variables are set
- GCS bucket access
- BigQuery access
- Credentials file exists

**Tests included:**
- ENV-2.1: Environment variables
- ENV-2.2: GCS bucket access
- ENV-2.3: BigQuery access
- ENV-2.4: Credentials file

**When to run:** After environment setup or credential changes

```bash
python run_tests.py --category env
```

### 3. Configuration Validation Tests (CFG)
**What it tests:**
- YAML configuration files are valid
- Tables and sites can be listed
- Validation and dry-run modes work

**Tests included:**
- CFG-1.1: Facebook configuration
- CFG-1.2: WordPress configuration
- CFG-1.3: Generic configuration
- CFG-1.4: List Facebook tables
- CFG-1.5: List Facebook sites
- CFG-1.6: Skip validation flag

**When to run:** After YAML config changes

```bash
python run_tests.py --category config
```

### 4. Facebook Extraction Tests (FB)
**What it tests:**
- Facebook extraction validation
- Error handling for invalid inputs
- Dry-run and validate-only modes

**Tests included:**
- FB-4.1: Dry run validation
- FB-4.2: Validate-only mode
- FB-4.3: Single site extraction (optional)
- FB-4.4: Invalid group handling
- FB-4.5: Invalid site ID handling

**When to run:** After Facebook extractor changes

```bash
python run_tests.py --category extraction
```

### 5. WordPress Extraction Tests (WP)
**What it tests:**
- WordPress extraction validation
- Error handling for invalid inputs
- Configuration validation

**Tests included:**
- WP-5.1: Dry run validation
- WP-5.2: Validate-only mode
- WP-5.3: Single site validation (optional)
- WP-5.4: Invalid group handling

**When to run:** After WordPress extractor changes

```bash
python run_tests.py --category extraction
```

### 6. Schema & Data Quality Tests (DQ)
**What it tests:**
- No duplicate primary keys
- Recent data exists in staging and production
- All active tables have descriptions

**Tests included:**
- DQ-6.1: Duplicate primary key validation
- DQ-6.2: Recent staging data
- DQ-6.3: Recent production data
- DQ-6.4: Table descriptions exist

**When to run:** After schema changes or data issues

```bash
python run_tests.py --category quality
```

### 7. Pipeline Manager Tests (PM)
**What it tests:**
- Pipeline manager commands work
- Job listing and health checks

**Tests included:**
- PM-5.1: List recent jobs
- PM-5.2: Pipeline health check
- PM-5.3: Health check for specific source
- PM-5.4: Cleanup dry run

**When to run:** After pipeline manager changes

```bash
python run_tests.py --category manager
```

### 8. Monitoring Tests (MON)
**What it tests:**
- Monitoring tables exist
- Execution tracking works
- Job metadata is recorded

**Tests included:**
- MON-8.1: Monitoring table exists
- MON-8.2: Job metadata tracking

**When to run:** After monitoring changes

```bash
python run_tests.py --category monitoring
```

### 9. Error Handling Tests (ERR)
**What it tests:**
- Invalid inputs fail gracefully
- Error messages are helpful
- No uncaught exceptions

**Tests included:**
- ERR-9.1: Invalid source
- ERR-9.2: Invalid table
- ERR-9.3: Missing parameters
- ERR-9.4: Invalid date format

**When to run:** After error handling changes

```bash
python run_tests.py --category error
```

### 10. Integration Tests (INT)
**What it tests:**
- End-to-end data flow
- Data exists in staging and production

**Tests included:**
- INT-10.1: E2E pipeline validation

**When to run:** Before major releases (optional)

```bash
python run_tests.py --category integration
```

## CI/CD Workflow

### Recommended Workflow

1. **Before Commit:**
   ```bash
   # Quick syntax check
   python run_tests.py --category syntax
   ```

2. **After Commit (Local):**
   ```bash
   # Run CI/CD tests
   python run_tests.py
   ```

3. **Before Push:**
   ```bash
   # Run comprehensive tests
   python run_tests.py --category all
   ```

4. **After Push (CI/CD Pipeline):**
   ```bash
   # Automated CI/CD tests
   python run_tests.py --ci-mode
   ```

### GitHub Actions Integration

Create `.github/workflows/test.yml`:

```yaml
name: CI/CD Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v3

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'

    - name: Install dependencies
      run: |
        pip install -r requirements.txt

    - name: Run CI/CD tests
      env:
        GCP_PROJECT_ID: ${{ secrets.GCP_PROJECT_ID }}
        GOOGLE_APPLICATION_CREDENTIALS: ${{ secrets.GOOGLE_APPLICATION_CREDENTIALS }}
      run: |
        python run_tests.py --ci-mode
```

## Test Output

### Test Results Format

```
======================================================================
                    TEST RESULTS SUMMARY
======================================================================

Execution Time: 45.23 seconds

Total Tests: 25
Passed: 23
Failed: 0
Skipped: 2

Pass Rate: 100.0%
```

### JSON Report

Each test run generates a JSON report:
```
test_results_20251023_104532.json
```

Contains:
- Start/end time
- Duration
- Test counts
- Pass rate
- Detailed results for each test

## Exit Codes

- **0**: All tests passed
- **1**: One or more tests failed

Use in scripts:
```bash
python run_tests.py
if [ $? -eq 0 ]; then
    echo "Tests passed, proceeding with deployment"
    # deployment commands here
else
    echo "Tests failed, aborting deployment"
    exit 1
fi
```

## Troubleshooting

### Common Issues

1. **Missing Environment Variables**
   ```
   Error: Missing env vars: GCP_PROJECT_ID, GOOGLE_APPLICATION_CREDENTIALS
   ```
   **Solution:** Set required environment variables in `.env` or shell

2. **Import Errors**
   ```
   Error: No module named 'facebook_business'
   ```
   **Solution:** Install dependencies: `pip install -r requirements.txt`

3. **BigQuery Access Denied**
   ```
   Error: Access Denied: Project bi-data-391216
   ```
   **Solution:** Verify credentials and project permissions

4. **Tests Timing Out**
   ```
   Error: Command timed out after 300s
   ```
   **Solution:** Skip extraction tests or increase timeout

### Debug Mode

Run with verbose output to see detailed logs:
```bash
python run_tests.py --verbose
```

## Best Practices

1. **Run CI/CD tests after every commit**
   - Fast execution (< 2 minutes)
   - Catches critical issues early

2. **Run full tests before pushing to main**
   - Comprehensive validation
   - Includes optional checks

3. **Review test reports**
   - Check JSON reports for trends
   - Investigate skipped tests

4. **Update tests when adding features**
   - Add new test cases for new functionality
   - Update existing tests for changes

5. **Don't skip failing tests**
   - Fix the underlying issue
   - Don't just disable the test

## Extending Tests

### Add New Test Category

1. **Create test function:**
```python
def test_my_category(results):
    """Test my new functionality."""
    print_header("MY CATEGORY TESTS")

    run_test(
        results,
        "MY-1.1",
        "Test my feature",
        "python my_script.py --test"
    )
```

2. **Add to main():**
```python
if args.category in ['all', 'mycategory']:
    test_my_category(results)
```

3. **Update choices:**
```python
parser.add_argument('--category',
    choices=['all', ..., 'mycategory'],
    ...)
```

### Add Validation Function

```python
def my_validation():
    # Custom validation logic
    if condition:
        return True, None
    else:
        return False, "Validation failed: reason"

run_test(
    results,
    "TEST-1",
    "My test with validation",
    "echo Running...",
    validation_func=my_validation
)
```

## Support

For issues with the test suite:
1. Check test output and JSON report
2. Run with `--verbose` for detailed logs
3. Review [MODULE_MANUAL.md](MODULE_MANUAL.md) for code reference
4. Contact data-team@company.com

---

**Last Updated:** 2025-10-23
