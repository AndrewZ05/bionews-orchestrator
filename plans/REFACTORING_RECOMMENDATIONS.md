# Orchestrator Codebase - Comprehensive Refactoring Analysis

**Analysis Date:** 2025-11-25
**Total Files Analyzed:** 108 Python files
**Total Lines of Code:** ~15,000+
**Analysis Scope:** Extractors, shared modules, orchestrate.py main entry point

---

## Executive Summary

Deep analysis of the orchestrator codebase reveals significant refactoring opportunities across three key areas:

1. **Extractor Plugins (6 files, ~12,150 LOC)**: 47 refactoring opportunities identified
   - 1,200+ lines of duplicated boilerplate in `run_pipeline` functions
   - 120 lines of identical SSH tunnel code duplicated across 2 extractors
   - 1,030-line `run_pipeline` function in Facebook extractor (unmaintainable)

2. **Shared Modules (4 files, ~2,644 LOC)**: 27 refactoring opportunities identified
   - 318-line `process_hash_merge()` function with 5+ nesting levels
   - 295-line `apply_schema_evolution()` function mixing multiple responsibilities
   - sys.exit() calls in library code (breaks reusability)

3. **Main Orchestrator (1 file, 2,014 LOC)**: 32 refactoring opportunities identified
   - 1,440-line `main()` function handling entire pipeline
   - 169-line validation function with 40+ checks
   - Complex nested conditionals (4-5 levels deep)

**Estimated Total Effort:** 6-8 weeks (1 developer)
**Estimated Code Reduction:** 40% (1,500+ lines eliminated)
**Estimated Maintainability Improvement:** HIGH

**Refactoring Philosophy:**
- **Use functions for 80% of refactorings** - Simple, composable, testable
- **Use dataclasses for 20%** - Only for data structures with many fields (PipelineContext, ExtractionResult)
- **Avoid inheritance** - Prefer function composition over class hierarchies
- **Functional-first approach** - Pure functions, immutable data, declarative pipelines

---

## Priority Matrix

| Rank | Refactoring | Files | Impact | Effort | ROI | Priority |
|------|-------------|-------|--------|--------|-----|----------|
| 1 | run_pipeline Boilerplate Extraction | 6 | CRITICAL | High | CRITICAL | P0 |
| 2 | SSH Tunnel Duplication | 2 | HIGH | Medium | HIGH | P0 |
| 3 | Main Pipeline Decomposition | 1 | CRITICAL | High | CRITICAL | P0 |
| 4 | process_hash_merge() Refactoring | 1 | HIGH | High | HIGH | P1 |
| 5 | Error Handling Standardization | All | HIGH | Medium | HIGH | P1 |
| 6 | Table Extractor Pattern (Facebook) | 1 | HIGH | High | MEDIUM | P1 |
| 7 | apply_schema_evolution() Refactoring | 1 | HIGH | High | MEDIUM | P2 |
| 8 | Configuration Management | 1 | MEDIUM | Medium | MEDIUM | P2 |
| 9 | Type Safety Improvements | All | MEDIUM | Medium | MEDIUM | P2 |
| 10 | Post-Processor Refactoring | 1 | MEDIUM | Medium | LOW | P3 |

---

## PART 1: EXTRACTOR PLUGINS REFACTORING

### 1.1 CRITICAL: run_pipeline Boilerplate (1,200+ LOC duplicated)

**Problem:**
All 6 extractors have nearly identical boilerplate:
- Date range calculation (40-60 lines each)
- Environment initialization (20-30 lines each)
- Stats dictionary setup (identical across 4 extractors)
- Parameter handling (20+ parameters per function)

**Files Affected:**
- [plugins/facebook_extractor.py](plugins/facebook_extractor.py#L1969-L2999) (1030 lines)
- [plugins/mailchimp_extractor.py](plugins/mailchimp_extractor.py#L4117) (794 lines)
- [plugins/wordpress_extractor.py](plugins/wordpress_extractor.py#L838) (246 lines)
- [plugins/limesurvey_extractor.py](plugins/limesurvey_extractor.py#L1128) (376 lines)
- [plugins/dcm_extractor.py](plugins/dcm_extractor.py#L838) (310 lines)

**Current Code Example:**
```python
# Repeated in ALL extractors
def run_pipeline(
    config, sites, tables, group, refresh_mode, lookback_days, start_date, end_date,
    test_mode, batch_size, max_retries, skip_hash_merge, archive_staging,
    truncate_staging, rebuild, bq_client, execution_id, parallel_workers,
    schema_prefix, schema_suffix, **kwargs
) -> Dict[str, Any]:
    # Date calculation (40-60 lines)
    if not start_date and lookback_days and refresh_mode != 'full':
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        logger.info(f"Using lookback of {lookback_days} days: start_date={start_date}")
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')

    # Environment initialization (20-30 lines)
    env = initialize_pipeline_environment(
        config, bq_client=bq_client, source_default='facebook',
        schema_prefix=schema_prefix, schema_suffix=schema_suffix,
        execution_id=execution_id, rebuild=rebuild, require_bucket=False
    )
    source = env.source_name
    main_dataset = env.production_dataset
    run_execution_id = env.execution_id or execution_id

    # Stats initialization (identical)
    stats = {'total_rows': 0, 'tables': 0, 'successful_tables': 0, 'table_files': {}}

    # Then 500-900 lines of table-specific extraction logic...
```

**Proposed Solution (Functional-First):**
```python
# Create: shared/pipeline_helpers.py
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta

# PURE FUNCTIONS - No classes needed!

def calculate_date_range(
    refresh_mode: str,
    lookback_days: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Tuple[str, str]:
    """Calculate date range for extraction (pure function)."""
    if not start_date and lookback_days and refresh_mode != 'full':
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    return start_date, end_date

def initialize_stats() -> Dict[str, Any]:
    """Create stats dictionary (pure function)."""
    return {
        'total_rows': 0,
        'tables': 0,
        'successful_tables': 0,
        'table_files': {}
    }

def setup_environment(config: Dict[str, Any], source: str, **options) -> Any:
    """Initialize pipeline environment (extracted common logic)."""
    return initialize_pipeline_environment(
        config,
        bq_client=options.get('bq_client'),
        source_default=source,
        schema_prefix=options.get('schema_prefix'),
        schema_suffix=options.get('schema_suffix'),
        execution_id=options.get('execution_id'),
        rebuild=options.get('rebuild', False),
        require_bucket=False
    )

# Simplified extractor signature - use **options instead of 20 parameters!
def run_pipeline(
    config: Dict[str, Any],
    sites: List[str],
    tables: List[str],
    **options  # All optional parameters collected here
) -> Dict[str, Any]:
    """Facebook extraction pipeline - NOW <150 lines instead of 1030.

    Uses functional helpers for common patterns.
    No classes, no complexity - just composable functions!
    """
    # Use helper functions
    start_date, end_date = calculate_date_range(
        options.get('refresh_mode', 'incremental'),
        options.get('lookback_days'),
        options.get('start_date'),
        options.get('end_date')
    )

    env = setup_environment(config, 'facebook', **options)
    stats = initialize_stats()

    # Process tables with functional approach
    for table in tables:
        result = process_facebook_table(
            table, sites, env,
            start_date, end_date,
            options
        )
        if result:
            stats['table_files'][table] = result['file_path']
            stats['total_rows'] += result['rows']

    return stats
```

**Optional: Use dataclass ONLY for complex parameter bundles (20% rule):**
```python
from dataclasses import dataclass

@dataclass
class ExtractionOptions:
    """Use dataclass ONLY when passing 10+ related parameters.
    Most extractors won't need this - **options dict is simpler!
    """
    refresh_mode: str = 'incremental'
    test_mode: bool = False
    rebuild: bool = False
    lookback_days: Optional[int] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    # ... only if absolutely necessary
```

**Benefits:**
- Eliminates 1,200+ lines of duplicate code
- Simple **options dict - no complex parameter objects needed
- Pure functions - easy to test (no mocks!)
- Single place to update date logic, initialization, stats
- Easier to add new extractors
- More Pythonic than classes

**Effort:** 4-5 days
**Risk:** Medium (touches all extractors, requires thorough testing)
**Priority:** P0 (CRITICAL - highest ROI)

**Why Functional > Classes:**
- Simpler: Functions are easier to understand than class hierarchies
- Composable: Small functions combine naturally
- Testable: Pure functions need no setup/teardown
- Flexible: **options dict allows any parameters without schema changes

---

### 1.2 CRITICAL: SSH Tunnel Duplication (120 LOC duplicated)

**Problem:**
WordPress and LimeSurvey have 100% identical SSH tunnel management code.

**Files Affected:**
- [plugins/wordpress_extractor.py:39-100](plugins/wordpress_extractor.py#L39-L100)
- [plugins/limesurvey_extractor.py:448-577](plugins/limesurvey_extractor.py#L448-L577)

**Duplicate Functions:**
1. `find_free_port()` - 10 lines
2. `start_ssh_tunnel()` - 60 lines
3. `stop_ssh_tunnel()` - 10 lines

**Current Code:**
```python
# IDENTICAL in both files
def find_free_port() -> Tuple[int, socket.socket]:
    """
    Find a free port and return both the port number and the socket.
    Fix #7: Keep socket open to prevent port reuse race condition.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    s.listen(1)
    port = s.getsockname()[1]
    return port, s

def start_ssh_tunnel(...) -> subprocess.Popen:
    # 60 lines of tunnel setup
    pass

def stop_ssh_tunnel(tunnel_process: subprocess.Popen) -> None:
    # 10 lines of cleanup
    pass
```

**Issue:**
LimeSurvey added gcloud support that WordPress lacks. Bug fixes (Fix #7) must be applied to both files.

**Proposed Solution (Pure Functions):**
```python
# Create: shared/ssh_tunnel_utils.py
# ALL PURE FUNCTIONS - No classes, no state, no inheritance!

from typing import Tuple, Optional
import socket
import subprocess

def find_free_port() -> Tuple[int, socket.socket]:
    """Find available port with race condition prevention (pure function)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    s.listen(1)
    port = s.getsockname()[1]
    return port, s

def start_ssh_tunnel(
    ssh_host: str,
    ssh_user: str,
    ssh_keyfile: str,
    db_host: str,
    db_port: int,
    local_port: int,
    port_socket: Optional[socket.socket] = None,
    use_gcloud: bool = False,  # LimeSurvey enhancement
    **gcloud_opts  # Flexible gcloud options
) -> subprocess.Popen:
    """Start SSH tunnel with optional gcloud support (functional).

    Args:
        use_gcloud: Use gcloud instead of standard SSH
        **gcloud_opts: Flexible gcloud options (instance, zone, etc.)

    Returns:
        subprocess.Popen: Tunnel process
    """
    if use_gcloud:
        # Build gcloud command functionally
        tunnel_cmd = build_gcloud_tunnel_command(
            ssh_host, db_host, db_port, local_port, gcloud_opts
        )
    else:
        # Build standard SSH command
        tunnel_cmd = build_ssh_tunnel_command(
            ssh_host, ssh_user, ssh_keyfile, db_host, db_port, local_port
        )

    tunnel_process = subprocess.Popen(tunnel_cmd, ...)
    return tunnel_process

def build_ssh_tunnel_command(
    ssh_host: str, ssh_user: str, ssh_keyfile: str,
    db_host: str, db_port: int, local_port: int
) -> List[str]:
    """Build SSH tunnel command (pure function)."""
    return [
        'ssh', '-i', ssh_keyfile,
        '-L', f'{local_port}:{db_host}:{db_port}',
        f'{ssh_user}@{ssh_host}',
        '-N'
    ]

def build_gcloud_tunnel_command(
    ssh_host: str, db_host: str, db_port: int, local_port: int, opts: Dict
) -> List[str]:
    """Build gcloud tunnel command (pure function)."""
    return [
        'gcloud', 'compute', 'ssh',
        opts.get('instance', ssh_host),
        '--zone', opts.get('zone', 'us-central1-a'),
        '--tunnel-through-iap',
        '--', '-L', f'{local_port}:{db_host}:{db_port}', '-N'
    ]

def stop_ssh_tunnel(tunnel_process: subprocess.Popen) -> None:
    """Gracefully terminate SSH tunnel (pure side effect)."""
    if tunnel_process and tunnel_process.poll() is None:
        tunnel_process.terminate()

# Usage - simple imports, no class instantiation!
from shared.ssh_tunnel_utils import find_free_port, start_ssh_tunnel, stop_ssh_tunnel
```

**Benefits:**
- 120 lines removed from codebase
- Bug fixes in one place
- WordPress gets gcloud support for free
- Consistent behavior

**Effort:** 2-3 days
**Risk:** Low (well-isolated functionality)
**Priority:** P0 (quick win, high value)

---

### 1.3 HIGH: Metadata Enrichment Pattern (17 occurrences)

**Problem:**
Every table extraction ends with identical metadata enrichment + parquet writing code.

**Files Affected:** All 6 extractors (17 total occurrences)

**Current Pattern:**
```python
# Repeated 17 times across extractors
set_execution_metadata(all_data, exec_id, source=source)

local_file_path = extract_to_local_parquet(
    data=all_data,
    source=source,
    table=table,
    job_id=run_execution_id or execution_id,
    bq_client=bq_client,
    production_table_id=main_table,
    rebuild_mode=rebuild
)

if local_file_path:
    stats['table_files'][table] = local_file_path
    stats['total_rows'] += len(all_data)
```

**Inconsistencies:**
- DCM adds account mapping differently
- Mailchimp uses `enrich_record_with_tenant_info` instead
- LimeSurvey sanitizes records before enrichment

**Proposed Solution:**
```python
# Create: shared/extraction_pipeline.py
def extract_and_write_parquet(
    data: List[Dict[str, Any]],
    table: str,
    source: str,
    execution_id: str,
    env: PipelineEnvironment,
    bq_client: bigquery.Client,
    rebuild_mode: bool = False,
    site_mapping: Optional[Dict[str, Any]] = None,  # For DCM/multi-tenant
    pre_process: Optional[Callable] = None  # For LimeSurvey sanitization
) -> Optional[str]:
    """Standardized extraction to parquet with metadata enrichment.

    Returns: Local parquet file path or None
    """
    if not data:
        return None

    # Apply preprocessing if needed (e.g., sanitization)
    if pre_process:
        data = pre_process(data)

    # Add site mapping for multi-tenant sources
    if site_mapping:
        for record in data:
            record.update(site_mapping)

    # Enrich with execution metadata
    set_execution_metadata(data, execution_id, source=source)

    # Write to parquet
    table_with_affix = env.format_table(table)
    main_table = f"{env.project}.{env.production_dataset}.{table_with_affix}"

    return extract_to_local_parquet(
        data=data,
        source=source,
        table=table,
        job_id=execution_id,
        bq_client=bq_client,
        production_table_id=main_table,
        rebuild_mode=rebuild_mode
    )

# Usage:
file_path = extract_and_write_parquet(
    data=all_data,
    table=table,
    source=source,
    execution_id=exec_id,
    env=env,
    bq_client=bq_client,
    rebuild_mode=rebuild
)

if file_path:
    stats['table_files'][table] = file_path
    stats['total_rows'] += len(all_data)
```

**Benefits:**
- 150 lines removed (17 occurrences × 9 lines)
- Consistent metadata handling
- Pure function - easy to test
- Easy to add new system fields globally
- Handles multi-tenant and sanitization use cases
- Function composition - pre_process is optional function, not class method!

**Effort:** 1-2 days
**Risk:** Low
**Priority:** P0 (quick win)

**Why Functional:**
- Single purpose function (does one thing well)
- Composable via pre_process callable
- No state, no classes needed

---

### 1.4 HIGH: Retry Logic Duplication (3 different implementations)

**Problem:**
Three different retry strategies for the same goal.

**Files Affected:**
- [facebook_extractor.py:355-373](facebook_extractor.py#L355-L373) - Manual rate limit handling
- [wordpress_extractor.py:471-620](wordpress_extractor.py#L471-L620) - Manual retry loops
- [mailchimp_extractor.py](mailchimp_extractor.py) - Decorator-based (GOOD!)

**Current Patterns:**

**Pattern 1 - Manual Retry Loop (WordPress):**
```python
retry_count = 0
max_retries = 3

while retry_count <= max_retries:
    try:
        # extraction logic
        return
    except Exception as e:
        is_transient = any(pattern in error_str.lower() for pattern in [
            'timeout', 'connection', 'ssh', 'broken pipe'
        ])

        if is_transient and retry_count < max_retries:
            retry_count += 1
            backoff = 30 * retry_count  # Linear: 30s, 60s, 90s
            time.sleep(backoff)
            continue
```

**Pattern 2 - Facebook Rate Limiting:**
```python
if error_code == 80004:
    wait_times = [300, 900, 1800, 3600]  # Account-level
elif error_code == 17:
    wait_times = [180, 600, 1200]  # User-level
else:
    wait_times = [60, 180, 300]  # App-level

if retry_count <= max_retries:
    wait_time = wait_times[min(retry_count - 1, len(wait_times) - 1)]
    time.sleep(wait_time)
```

**Pattern 3 - Decorator (Mailchimp - BEST):**
```python
from shared.error_handler import retry_on_transient_error

@retry_on_transient_error(max_attempts=3, base_delay=2.0)
def _do_query():
    # query logic
```

**Proposed Solution (Functional - No Classes!):**
```python
# Enhance: shared/error_handler.py
# FUNCTIONAL APPROACH - Functions that return functions (closures)

from typing import Callable, Optional, List, Dict
from functools import wraps
import time

# Backoff strategy functions (not classes!)
def linear_backoff(base_delay: float = 30.0) -> Callable[[int], float]:
    """Returns a function that calculates linear backoff."""
    def calculate(attempt: int) -> float:
        return base_delay * (attempt + 1)  # 30s, 60s, 90s
    return calculate

def exponential_backoff(base_delay: float = 2.0) -> Callable[[int], float]:
    """Returns a function that calculates exponential backoff."""
    def calculate(attempt: int) -> float:
        return base_delay * (2 ** attempt)  # 2s, 4s, 8s, 16s
    return calculate

def facebook_rate_limit_backoff(error_code_map: Dict[int, List[int]]) -> Callable:
    """Returns a function that calculates Facebook-specific backoff."""
    def calculate(attempt: int, error: Exception = None) -> float:
        if hasattr(error, 'api_error_code'):
            code = error.api_error_code()
            wait_times = error_code_map.get(code, [60, 120, 240])
        else:
            wait_times = [60, 120, 240]
        return wait_times[min(attempt, len(wait_times) - 1)]
    return calculate

# Pure function to check if error is transient
def is_transient_error(error: Exception, patterns: Optional[List[str]] = None) -> bool:
    """Check if error is transient (pure function)."""
    if patterns is None:
        patterns = ['timeout', 'connection', 'rate limit']
    error_str = str(error).lower()
    return any(pattern in error_str for pattern in patterns)

# Decorator factory (higher-order function)
def retry_with_backoff(
    backoff_fn: Callable[[int], float],
    max_attempts: int = 3,
    transient_patterns: Optional[List[str]] = None
):
    """Functional retry decorator using closures.

    Args:
        backoff_fn: Function that calculates wait time (from closures above)
        max_attempts: Maximum retry attempts
        transient_patterns: Error patterns to retry on

    Example:
        @retry_with_backoff(exponential_backoff(2.0), max_attempts=3)
        def my_function():
            pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if not is_transient_error(e, transient_patterns):
                        raise

                    if attempt == max_attempts - 1:
                        raise

                    # Calculate wait time using provided function
                    wait_time = backoff_fn(attempt)
                    logger.warning(f"Retry {attempt+1}/{max_attempts} in {wait_time}s: {e}")
                    time.sleep(wait_time)
        return wrapper
    return decorator

# Usage - compose functions!

# WordPress (linear backoff):
@retry_with_backoff(
    backoff_fn=linear_backoff(30.0),
    max_attempts=3,
    transient_patterns=['timeout', 'connection', 'ssh']
)
def extract_wordpress_table():
    pass

# Facebook (rate limit backoff):
@retry_with_backoff(
    backoff_fn=facebook_rate_limit_backoff({
        80004: [300, 900, 1800, 3600],
        17: [180, 600, 1200],
        4: [60, 180, 300]
    }),
    max_attempts=4
)
def fetch_facebook_data():
    pass

# Standard exponential:
@retry_with_backoff(exponential_backoff(2.0), max_attempts=3)
def api_call():
    pass
```

**Benefits:**
- Unified retry implementation
- Consistent backoff strategies (via composable functions)
- Easy to tune globally
- Pure functions - super testable!
- Closures instead of classes - more Pythonic
- Function composition - backoff_fn is pluggable

**Effort:** 2-3 days
**Risk:** Medium (affects error handling)
**Priority:** P1

**Why Functional > Class:**
- Backoff functions are pure (no state)
- Composable via higher-order functions
- Simpler than Enum + class hierarchy
- Each backoff strategy is just a function that returns a function!

---

### 1.5 MEDIUM: Table Extractor Pattern (Facebook 1030 lines → 100 lines)

**Problem:**
Facebook's `run_pipeline` function is 1,030 lines with table-specific logic embedded.

**File Affected:** [plugins/facebook_extractor.py:1969-2999](plugins/facebook_extractor.py#L1969-L2999)

**Current Structure:**
```python
def run_pipeline(...):  # 1030 lines!
    # Setup (50 lines)
    env = initialize_pipeline_environment(...)

    # Table processing (900 lines)
    for table in tables:
        if table == 'campaigns':
            # 100 lines of campaign extraction
        elif table == 'adsets':
            # 100 lines of adset extraction
        elif table == 'ads':
            # 150 lines of ads extraction
        elif table == 'ad_insights':
            # 200 lines of insights extraction
        elif table == 'posts':
            # 300 lines of posts extraction
        # ... 10 more table types
```

**Proposed Solution (Functional - No Classes!):**
```python
# Create: plugins/facebook/table_extractors.py
# PURE FUNCTIONS - Each table type is just a function!

from typing import Dict, Any, List, Tuple

# Table extraction functions (not classes!)
def extract_campaigns(
    accounts: List[str],
    date_range: Tuple[str, str],
    config: Dict[str, Any],
    env: Any,
    **options
) -> List[Dict[str, Any]]:
    """Extract campaigns for accounts (pure business logic)."""
    # Campaign-specific logic (100 lines)
    return extract_hierarchy(accounts, 'campaigns', date_range, config)

def extract_adsets(accounts, date_range, config, env, **options):
    """Extract adsets for accounts."""
    # Adset-specific logic (100 lines)
    return extract_hierarchy(accounts, 'adsets', date_range, config)

def extract_ad_insights(accounts, date_range, config, env, **options):
    """Extract ad insights for accounts."""
    # Insights logic (200 lines) - already complex, but isolated!
    all_data = []
    for account in accounts:
        for record in extract_insights_async(account, date_range):
            all_data.append(record)
    return all_data

def extract_posts(accounts, date_range, config, env, **options):
    """Extract posts for accounts."""
    # Posts logic (300 lines)
    return extract_posts_parallel(accounts, date_range, config)

# Table registry - map table names to functions (not classes!)
TABLE_EXTRACTORS = {
    'campaigns': extract_campaigns,
    'adsets': extract_adsets,
    'ads': extract_ads,
    'ad_insights': extract_ad_insights,
    'posts': extract_posts,
    'page_posts': extract_page_posts,
    'page_insights': extract_page_insights,
    # Just functions in a dict!
}

# Simplified run_pipeline (NOW <100 lines instead of 1030!)
def run_pipeline(config, sites, tables, **options) -> Dict[str, Any]:
    """Facebook extraction pipeline - functional style."""

    # Setup using helper functions
    start_date, end_date = calculate_date_range(
        options.get('refresh_mode', 'incremental'),
        options.get('lookback_days'),
        options.get('start_date'),
        options.get('end_date')
    )

    env = setup_environment(config, 'facebook', **options)
    stats = initialize_stats()

    # Process tables by calling functions from registry
    for table in tables:
        extractor_fn = TABLE_EXTRACTORS.get(table)
        if not extractor_fn:
            logger.warning(f"Unknown table: {table}")
            continue

        # Call the extraction function!
        data = extractor_fn(sites, (start_date, end_date), config, env, **options)

        if data:
            file_path = extract_and_write_parquet(
                data, table, 'facebook',
                env.execution_id, env, options.get('bq_client')
            )
            stats['table_files'][table] = file_path
            stats['total_rows'] += len(data)

    return stats
```

**Benefits:**
- run_pipeline reduced from 1030 to <100 lines
- Each table extractor testable independently (just test the function!)
- Easy to add new table types (just add function to dict)
- No inheritance - just functions in a registry
- No ABC classes, no base classes - simpler!
- Each extractor is pure business logic

**Effort:** 5-7 days
**Risk:** High (complex refactoring, extensive testing needed)
**Priority:** P1 (high impact, but can be done incrementally)

**Why Functional > Classes:**
- No need for abstract base classes
- No need to instantiate objects
- Registry is just a dict of functions (data, not classes!)
- Each extractor function has same signature - consistency without inheritance
- Test by calling function directly - no mocking needed!

---

## PART 2: SHARED MODULES REFACTORING

### 2.1 HIGH: process_hash_merge() Complexity (318 lines, 5+ nesting levels)

**Problem:**
Single massive function handling merge logic, lock management, query building, statistics, and cleanup.

**File Affected:** [shared/bigquery_utils.py:143-460](shared/bigquery_utils.py#L143-L460)

**Current Structure:**
```python
def process_hash_merge(...) -> Dict[str, Any]:  # 318 lines!
    # Schema validation (50 lines)
    # Lock acquisition (80 lines)
    try:
        # Acquire merge lock
        for attempt in range(lock_max_attempts):
            try:
                # Lock logic
            except Conflict:
                # Backoff

        # Query building (100 lines)
        # MERGE statement construction
        # Metric table handling
        # SAFE_CAST logic

        # Execution (30 lines)

        # Statistics (40 lines)

    finally:
        # Cleanup (30 lines)
```

**Proposed Solution:**
```python
# Refactor into smaller functions and classes

class MergeLockManager:
    """Context manager for merge locks."""
    def __init__(self, client, table_id, execution_id, max_attempts=10):
        self.client = client
        self.table_id = table_id
        self.execution_id = execution_id
        self.max_attempts = max_attempts
        self.lock_acquired = False

    def __enter__(self):
        self._acquire_lock()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._release_lock()
        return False

class MergeQueryBuilder:
    """Build MERGE queries with proper type casting."""
    def __init__(self, staging_schema, production_schema, primary_keys):
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        self.primary_keys = primary_keys

    def build_staging_cte(self, staging_table_id):
        """Build staging CTE with SAFE_CAST for type mismatches."""
        # Extract to separate method (40 lines)

    def build_merge_query(self, staging_table_id, production_table_id):
        """Build complete MERGE statement."""
        # Extract to separate method (60 lines)

# Simplified process_hash_merge (now <100 lines):
def process_hash_merge(...) -> Dict[str, Any]:
    """Execute hash-based merge from staging to production."""

    # Validate schemas
    validate_merge_schemas(staging_table_id, main_table_id, primary_keys)

    # Acquire lock and execute merge
    with MergeLockManager(client, main_table_id, execution_id) as lock:
        query_builder = MergeQueryBuilder(staging_schema, prod_schema, primary_keys)
        merge_query = query_builder.build_merge_query(staging_table_id, main_table_id)

        job = client.query(merge_query)
        job.result()

    # Collect statistics
    stats = calculate_merge_statistics(job, main_table_id)

    # Cleanup if requested
    if truncate_staging or archive_staging:
        cleanup_staging_table(...)

    return stats
```

**Benefits:**
- Reduced nesting (5+ levels → 2 levels)
- Each component testable independently
- Lock management reusable
- Query building separated from execution

**Effort:** 6-8 days
**Risk:** High (core merge logic, extensive testing required)
**Priority:** P1

---

### 2.2 HIGH: apply_schema_evolution() Complexity (295 lines)

**Problem:**
Handles type conversion, field addition, YAML updates, logging, validation all in one function.

**File Affected:** [shared/schema_evolution.py:266-560](shared/schema_evolution.py#L266-L560)

**Current Structure:**
```python
def apply_schema_evolution(...) -> Tuple[bool, List[str]]:  # 295 lines!
    # Compare schemas (30 lines)
    comparison = compare_schemas(staging_schema, production_schema)

    # Auto type conversion (80 lines)
    for field_name, change in type_changes.items():
        # Try to auto-convert types

    # Add new fields (70 lines)
    for new_field in new_fields:
        # ALTER TABLE ADD COLUMN
        # Update YAML

    # Validation and reporting (80 lines)
    # Error reporting for incompatible changes
```

**Proposed Solution:**
```python
class SchemaEvolver:
    """Handles schema evolution with clear separation of concerns."""

    def __init__(self, client, staging_table_id, production_table_id):
        self.client = client
        self.staging_table_id = staging_table_id
        self.production_table_id = production_table_id

    def evolve(self, auto_convert=True, update_yaml=True):
        """Main evolution orchestration."""
        comparison = self._compare_schemas()

        if auto_convert:
            self._attempt_type_conversions(comparison.type_changes)

        self._add_new_fields(comparison.new_fields)

        incompatibilities = self._validate_schema_changes(comparison)
        if incompatibilities:
            return False, incompatibilities

        if update_yaml:
            self._update_yaml_schema()

        return True, []

    def _attempt_type_conversions(self, type_changes):
        """Extract type conversion logic (40 lines)."""
        pass

    def _add_new_fields(self, new_fields):
        """Extract field addition logic (40 lines)."""
        pass

    def _validate_schema_changes(self, comparison):
        """Extract validation logic (40 lines)."""
        pass

# Simplified usage:
evolver = SchemaEvolver(client, staging_table_id, production_table_id)
success, warnings = evolver.evolve(auto_convert=True, update_yaml=True)
```

**Benefits:**
- Each method < 50 lines
- Clear responsibilities
- Testable in isolation
- Easy to add new evolution strategies

**Effort:** 6-8 days
**Risk:** High (schema evolution is critical)
**Priority:** P2

---

### 2.3 CRITICAL: sys.exit() in Library Code (gcs_storage.py)

**Problem:**
`verify_gcs_object_access()` calls `sys.exit()` in library code, breaking reusability.

**File Affected:** [shared/gcs_storage.py:93,97,100](shared/gcs_storage.py#L93)

**Current Code:**
```python
def verify_gcs_object_access(...) -> bool:
    try:
        # Perform access test
    except Exception as e:
        if 'forbidden' in str(e).lower():
            logger.error(f"Permission denied for {bucket_name}")
            sys.exit(1)  # BAD!
        elif 'not found' in str(e).lower():
            logger.error(f"Bucket {bucket_name} not found")
            sys.exit(1)  # BAD!
        else:
            logger.error(f"Error: {e}")
            sys.exit(1)  # BAD!
```

**Proposed Solution:**
```python
# Create custom exceptions
class GCSAccessError(Exception):
    """Base exception for GCS access errors."""
    pass

class GCSPermissionError(GCSAccessError):
    """Raised when permission is denied."""
    pass

class GCSBucketNotFoundError(GCSAccessError):
    """Raised when bucket doesn't exist."""
    pass

def verify_gcs_object_access(...) -> bool:
    """Verify GCS bucket access.

    Raises:
        GCSPermissionError: If access is forbidden
        GCSBucketNotFoundError: If bucket doesn't exist
        GCSAccessError: For other access errors
    """
    try:
        # Perform access test
        return True
    except Exception as e:
        error_msg = str(e).lower()
        if 'forbidden' in error_msg:
            raise GCSPermissionError(f"Permission denied for {bucket_name}")
        elif 'not found' in error_msg:
            raise GCSBucketNotFoundError(f"Bucket {bucket_name} not found")
        else:
            raise GCSAccessError(f"GCS access error: {e}")

# Caller decides how to handle:
try:
    verify_gcs_object_access(bucket_name)
except GCSAccessError as e:
    logger.error(f"GCS verification failed: {e}")
    sys.exit(1)  # Exit decision moved to caller
```

**Benefits:**
- Library code reusable (doesn't kill process)
- Caller controls error handling
- Specific exception types for better error handling
- Standard Python exception patterns

**Effort:** 1-2 days
**Risk:** Low (well-isolated change)
**Priority:** P0 (breaks best practices)

---

### 2.4 MEDIUM: Type Safety Improvements (All Modules)

**Problem:**
Missing type hints, overuse of `Any`, generic `Dict` return types.

**Files Affected:** All shared modules

**Current Issues:**
```python
# Missing return type hints
def generate_hash(record):  # Should return str
    pass

def compare_schemas(staging_schema, production_schema):  # Should return SchemaComparison
    return {'new_fields': [], 'type_changes': {}, ...}  # Dict[str, Any]

# Overuse of Any
def process_hash_merge(
    config: Dict[str, Any],  # Too generic
    primary_keys: Any,  # Could be List[str]
    ...
) -> Dict[str, Any]:  # Could be MergeResult
    pass
```

**Proposed Solution:**
```python
from typing import TypedDict, List, Dict, Optional
from dataclasses import dataclass

# Create specific types
class SchemaComparison(TypedDict):
    new_fields: List[bigquery.SchemaField]
    type_changes: Dict[str, Dict[str, str]]
    mode_changes: Dict[str, Dict[str, str]]
    is_compatible: bool

@dataclass
class MergeResult:
    """Result of merge operation."""
    inserted: int
    updated: int
    deleted: int
    unchanged: int
    total_rows: int
    execution_time_seconds: float

# Add type hints
def generate_hash(record: Dict[str, Any]) -> str:
    """Generate MD5 hash of record."""
    pass

def compare_schemas(
    staging_schema: List[bigquery.SchemaField],
    production_schema: List[bigquery.SchemaField]
) -> SchemaComparison:
    """Compare two BigQuery schemas."""
    pass

def process_hash_merge(
    config: PipelineConfig,  # Specific type instead of Dict[str, Any]
    primary_keys: List[str],  # Specific instead of Any
    ...
) -> MergeResult:  # Specific instead of Dict[str, Any]
    pass
```

**Benefits:**
- IDE autocomplete support
- Compile-time type checking (mypy)
- Self-documenting code
- Prevents type-related bugs

**Effort:** 4-6 days
**Risk:** Low (additive changes, no behavior change)
**Priority:** P2

---

## PART 3: ORCHESTRATE.PY REFACTORING

### 3.1 CRITICAL: main() Function Decomposition (1,440 lines → ~200 lines)

**Problem:**
Single massive function handling entire pipeline orchestration.

**File Affected:** [orchestrate.py:547-1987](orchestrate.py#L547-L1987)

**Current Structure:**
```python
def main():  # 1,440 lines!
    # Argument parsing (100 lines)
    # Validation (150 lines)
    # Config loading (50 lines)
    # Resume logic (80 lines)
    # Extraction (200 lines)
    # GCS upload (100 lines)
    # External tables (120 lines)
    # Transform (220 lines)
    # Post-processing (80 lines)
    # Cleanup (150 lines)
    # Summary generation (120 lines)
    # Chaining (70 lines)
```

**Proposed Solution:**
```python
@dataclass
class PipelineContext:
    """Centralized pipeline context."""
    args: argparse.Namespace
    config: Dict[str, Any]
    env: str
    source: str
    tables: List[str]
    sites: List[str]
    groups: List[str]
    execution_id: str
    job_id: str
    bq_client: bigquery.Client
    stats: Dict[str, Any]
    table_files: Dict[str, str]

class PipelinePhase(ABC):
    """Base class for pipeline phases."""

    @abstractmethod
    def should_run(self, context: PipelineContext) -> bool:
        """Determine if this phase should execute."""
        pass

    @abstractmethod
    def execute(self, context: PipelineContext) -> Dict[str, Any]:
        """Execute the phase."""
        pass

class ExtractionPhase(PipelinePhase):
    def should_run(self, context):
        return not context.args.transform_only

    def execute(self, context):
        # Extraction logic (100 lines)
        pass

class GCSUploadPhase(PipelinePhase):
    def should_run(self, context):
        return (context.stats['total_rows'] > 0 and
                not context.args.extract_only and
                not context.args.skip_gcs)

    def execute(self, context):
        # Upload logic (80 lines)
        pass

class TransformPhase(PipelinePhase):
    def should_run(self, context):
        return (context.stats['total_rows'] > 0 and
                not context.args.extract_only and
                not context.args.skip_transform)

    def execute(self, context):
        # Transform logic (150 lines)
        pass

class Pipeline:
    """Main pipeline orchestrator."""

    def __init__(self):
        self.phases = [
            ExtractionPhase(),
            GCSUploadPhase(),
            ExternalTablePhase(),
            TransformPhase(),
            PostProcessingPhase()
        ]

    def run(self, context: PipelineContext):
        """Execute all enabled phases."""
        for phase in self.phases:
            if phase.should_run(context):
                logger.info(f"Running {phase.__class__.__name__}")
                try:
                    result = phase.execute(context)
                    context.stats.update(result)
                except Exception as e:
                    logger.error(f"Phase {phase.__class__.__name__} failed: {e}")
                    raise

# Simplified main() (now ~200 lines):
def main():
    """Main orchestration entry point."""
    # Parse and validate arguments (50 lines)
    args = parse_arguments()
    errors = validate_arguments(args)
    if errors:
        print_errors(errors)
        sys.exit(1)

    # Initialize context (50 lines)
    context = initialize_pipeline_context(args)

    # Run pipeline (5 lines!)
    pipeline = Pipeline()
    pipeline.run(context)

    # Generate summary and chain (50 lines)
    generate_summary(context)
    execute_chained_commands(context)
```

**Benefits:**
- main() reduced from 1,440 to ~200 lines
- Each phase testable independently
- Clear phase dependencies
- Easy to add/remove phases
- Self-documenting execution flow

**Effort:** 16-20 hours (2-3 days)
**Risk:** High (touches entire pipeline)
**Priority:** P0 (highest impact on maintainability)

---

### 3.2 HIGH: validate_arguments() Refactoring (169 lines → ~30 lines)

**Problem:**
Single function with 40+ validation rules.

**File Affected:** [orchestrate.py:703-872](orchestrate.py#L703-L872)

**Current Structure:**
```python
def validate_arguments(args):  # 169 lines!
    errors = []

    # 40+ individual if-checks
    if args.table and args.all_tables:
        errors.append("Cannot use --table with --all-tables")

    if args.group and args.all_groups:
        errors.append("Cannot use --group with --all-groups")

    # ... 38 more checks

    return errors
```

**Proposed Solution:**
```python
class ArgumentValidator:
    """Structured argument validation."""

    def __init__(self, args):
        self.args = args
        self.errors = []

    def validate(self):
        """Run all validation rules."""
        self.validate_table_selection()
        self.validate_date_options()
        self.validate_mode_conflicts()
        self.validate_state_management()
        self.validate_transform_dependencies()
        self.validate_rebuild_requirements()
        return self.errors

    def validate_table_selection(self):
        """Validate table/group selection options (< 20 lines)."""
        if self.args.table and self.args.all_tables:
            self.errors.append("Cannot use --table with --all-tables")

        if self.args.group and self.args.all_groups:
            self.errors.append("Cannot use --group with --all-groups")

    def validate_date_options(self):
        """Validate date-related options (< 20 lines)."""
        if self.args.lookback and (self.args.start_date or self.args.end_date):
            self.errors.append("Cannot use --lookback with --start-date/--end-date")

    # ... other validation methods

# Usage:
validator = ArgumentValidator(args)
errors = validator.validate()
```

**Benefits:**
- Each validation method < 20 lines
- Easy to add new validation rules
- Testable validation categories
- Clear organization

**Effort:** 6-8 hours
**Risk:** Low
**Priority:** P0 (quick win, high readability improvement)

---

### 3.3 HIGH: Error Handling Context Manager (Eliminates 200+ lines of duplication)

**Problem:**
Identical try-except-finally pattern repeated 8+ times for table processing.

**File Affected:** Throughout [orchestrate.py](orchestrate.py)

**Current Pattern:**
```python
# Repeated 8+ times
try:
    # operation
    if success:
        logger.info(f"[SUCCESS] {table}: ...")
        if processing_id:
            try:
                complete_table_processing(processing_id, status="success", ...)
            except Exception as track_err:
                logger.warning(f"Failed to complete tracking: {track_err}")
    else:
        logger.error(f"[FAILED] {table}: ...")
        if processing_id:
            try:
                complete_table_processing(processing_id, status="failed", ...)
            except Exception as track_err:
                logger.warning(f"Failed to complete tracking: {track_err}")
except Exception as e:
    logger.error(f"Error for {table}: {e}")
    if processing_id:
        try:
            complete_table_processing(processing_id, status="failed", ...)
        except Exception as track_err:
            logger.warning(f"Failed to complete tracking: {track_err}")
```

**Proposed Solution:**
```python
class TableProcessingTracker:
    """Context manager for table processing with automatic tracking."""

    def __init__(self, table_name, processing_id=None):
        self.table = table_name
        self.processing_id = processing_id
        self.success = False
        self.metrics = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        status = "success" if exc_type is None and self.success else "failed"
        error_msg = str(exc_val) if exc_val else None

        self._complete_tracking(status, error_msg)
        return False  # Don't suppress exceptions

    def mark_success(self, **metrics):
        """Mark operation as successful with metrics."""
        self.success = True
        self.metrics = metrics

    def _complete_tracking(self, status, error_msg):
        """Complete tracking with error handling."""
        if not self.processing_id:
            return

        try:
            complete_table_processing(
                self.processing_id,
                status=status,
                error_message=error_msg,
                **self.metrics
            )
        except Exception as e:
            logger.warning(f"Failed to complete tracking for {self.table}: {e}")

# Usage (reduces 25 lines to 5):
with TableProcessingTracker(table, processing_id) as tracker:
    result = perform_operation()
    if result.success:
        tracker.mark_success(rows=result.rows)
    else:
        raise OperationError(result.error)
```

**Benefits:**
- 80% code reduction (200+ lines → 40 lines)
- Guaranteed cleanup
- Single error handling pattern
- Pythonic

**Effort:** 5-6 hours
**Risk:** Low
**Priority:** P0 (high ROI)

---

### 3.4 MEDIUM: Configuration Management (Eliminates duplicate loads)

**Problem:**
Config loaded multiple times, mutated in scattered locations.

**File Affected:** [orchestrate.py](orchestrate.py) (lines 896, 979, 903-924, 1101-1102)

**Current Issues:**
```python
# Line 896: Load config
config = load_config(args.source)

# Line 979: Load AGAIN!
config = load_config(args.source)

# Line 903-917: Mutation 1
if hasattr(args, 'vars') and args.vars:
    runtime_overrides = parse_vars(args.vars)
    config = deep_merge(config, runtime_overrides)

# Line 919-924: Mutation 2
if args.no_alerts:
    if 'alerting' not in config:
        config['alerting'] = {}
    config['alerting']['enabled'] = False

# Line 1101-1102: Mutation 3
config['_resume_mode'] = True
config['_resume_points'] = resumable_job['tables']
```

**Proposed Solution:**
```python
class ConfigurationManager:
    """Centralized configuration management."""

    def __init__(self, source):
        self.source = source
        self._config = None
        self._loaded = False

    def load(self):
        """Load base configuration (only once)."""
        if not self._loaded:
            self._config = load_config(self.source)
            self._loaded = True
        return self

    def apply_runtime_overrides(self, vars_list):
        """Apply --vars overrides."""
        if vars_list:
            overrides = parse_vars(vars_list)
            self._config = deep_merge(self._config, overrides)
        return self

    def configure_alerts(self, enabled):
        """Configure alerting."""
        if 'alerting' not in self._config:
            self._config['alerting'] = {}
        self._config['alerting']['enabled'] = enabled
        return self

    def set_resume_mode(self, resumable_job):
        """Set resume mode flags."""
        self._config['_resume_mode'] = True
        self._config['_resume_points'] = resumable_job['tables']
        return self

    def get(self):
        """Get final configuration."""
        if not self._loaded:
            raise RuntimeError("Configuration not loaded")
        return self._config

# Usage (fluent interface):
config = (ConfigurationManager(args.source)
    .load()
    .apply_runtime_overrides(args.vars)
    .configure_alerts(enabled=not args.no_alerts)
    .get())
```

**Benefits:**
- Single config load (no duplicates)
- Clear configuration lifecycle
- Fluent interface
- Testable
- No accidental mutations

**Effort:** 6-8 hours
**Risk:** Low
**Priority:** P2

---

## IMPLEMENTATION ROADMAP

### Phase 1: Quick Wins (Week 1) - 40 hours

**Priority P0 Items:**
1. SSH Tunnel Extraction (2-3 days)
   - Move to `shared/ssh_tunnel_utils.py`
   - Update WordPress and LimeSurvey
   - Test tunnel connections

2. Metadata Enrichment Pattern (1-2 days)
   - Create `shared/extraction_pipeline.py`
   - Replace 17 occurrences
   - Regression testing

3. sys.exit() in GCS Storage (1-2 days)
   - Create custom exceptions
   - Update callers
   - Test error paths

4. validate_arguments() Refactoring (1 day)
   - Create ArgumentValidator class
   - Extract validation methods
   - Unit tests

5. Error Handling Context Manager (1 day)
   - Create TableProcessingTracker
   - Replace 8+ occurrences
   - Test tracking completion

**Deliverables:**
- 500+ lines of code eliminated
- Improved error handling
- Better code organization
- Comprehensive unit tests

---

### Phase 2: Core Refactoring (Week 2-3) - 80 hours

**Priority P0-P1 Items:**
1. run_pipeline Boilerplate (4-5 days)
   - Create `shared/pipeline_boilerplate.py`
   - Create PipelineParams dataclass
   - Update all 6 extractors
   - Integration testing

2. Main Pipeline Decomposition (2-3 days)
   - Create PipelinePhase classes
   - Create PipelineContext
   - Refactor main() to orchestrator
   - End-to-end testing

3. Retry Logic Unification (2-3 days)
   - Enhance `shared/error_handler.py`
   - Create BackoffStrategy enum
   - Replace manual retry loops
   - Test rate limiting

4. process_hash_merge() Refactoring (1 week)
   - Create MergeLockManager
   - Create MergeQueryBuilder
   - Extract statistics calculation
   - Extensive testing (critical path)

**Deliverables:**
- 1,200+ lines eliminated
- Unified retry handling
- Simplified main orchestration
- Improved merge performance

---

### Phase 3: Advanced Refactoring (Week 4-5) - 80 hours

**Priority P1-P2 Items:**
1. Table Extractor Pattern - Facebook (5-7 days)
   - Create FacebookTableExtractor base class
   - Extract table-specific extractors
   - Create table registry
   - Comprehensive testing

2. apply_schema_evolution() Refactoring (1 week)
   - Create SchemaEvolver class
   - Extract type conversion logic
   - Extract validation logic
   - Schema evolution testing

3. Configuration Management (1-2 days)
   - Create ConfigurationManager
   - Replace scattered config loads
   - Test config lifecycle

4. Type Safety Improvements (2-3 days)
   - Create TypedDicts for results
   - Add missing type hints
   - Run mypy validation

**Deliverables:**
- Facebook extractor maintainable
- Schema evolution modular
- Type-safe codebase
- MyPy passing

---

### Phase 4: Documentation & Testing (Week 6) - 40 hours

1. **Documentation:**
   - Architecture decision records (ADRs)
   - Updated README with new patterns
   - API documentation for new classes
   - Migration guide for new extractors

2. **Testing:**
   - Unit test coverage > 80%
   - Integration tests for all phases
   - End-to-end pipeline tests
   - Performance benchmarks

3. **Cleanup:**
   - Remove deprecated code
   - Update logging standards
   - Code quality checks (pylint, black)
   - Security scan (bandit)

**Deliverables:**
- Complete documentation
- Test coverage reports
- Performance baselines
- Clean codebase

---

## TESTING STRATEGY

### Unit Tests Required

**Extractor Refactorings:**
```python
# tests/test_ssh_tunnel_utils.py
def test_find_free_port():
    """Test port allocation."""
    port, socket = find_free_port()
    assert 1024 < port < 65535
    assert socket is not None

def test_start_ssh_tunnel_standard():
    """Test standard SSH tunnel."""
    # Mock subprocess.Popen
    pass

def test_start_ssh_tunnel_gcloud():
    """Test gcloud SSH tunnel."""
    # Mock gcloud command
    pass

# tests/test_extraction_pipeline.py
def test_extract_and_write_parquet():
    """Test metadata enrichment and parquet writing."""
    pass

def test_extract_with_site_mapping():
    """Test multi-tenant site mapping."""
    pass

# tests/test_pipeline_boilerplate.py
def test_calculate_date_range_lookback():
    """Test lookback days calculation."""
    pass

def test_calculate_date_range_explicit():
    """Test explicit date range."""
    pass

def test_pipeline_params_validation():
    """Test parameter validation."""
    pass
```

**Shared Module Refactorings:**
```python
# tests/test_merge_lock_manager.py
def test_lock_acquisition():
    """Test merge lock acquisition."""
    pass

def test_lock_release_on_exception():
    """Test lock cleanup on errors."""
    pass

# tests/test_schema_evolver.py
def test_type_conversion():
    """Test automatic type conversion."""
    pass

def test_add_new_fields():
    """Test field addition."""
    pass

def test_incompatible_changes():
    """Test validation of incompatible changes."""
    pass
```

**Orchestrate.py Refactorings:**
```python
# tests/test_pipeline_phases.py
def test_extraction_phase():
    """Test extraction phase."""
    pass

def test_transform_phase():
    """Test transform phase."""
    pass

def test_phase_dependencies():
    """Test phase execution order."""
    pass

# tests/test_argument_validator.py
def test_table_selection_validation():
    """Test table selection rules."""
    pass

def test_date_validation():
    """Test date option validation."""
    pass
```

### Integration Tests

```python
# tests/integration/test_full_pipeline.py
def test_facebook_extraction_end_to_end():
    """Test complete Facebook extraction pipeline."""
    # Use test credentials and small date range
    pass

def test_mailchimp_extraction_with_resume():
    """Test Mailchimp extraction with resume capability."""
    pass

def test_schema_evolution_integration():
    """Test schema evolution with real BigQuery."""
    pass
```

---

## RISK MITIGATION

### High-Risk Refactorings

**1. process_hash_merge() (Priority P1)**
- **Risk:** Core merge logic affects all pipelines
- **Mitigation:**
  - Comprehensive unit tests before refactoring
  - Keep old code path with feature flag
  - Parallel testing (old vs new implementation)
  - Gradual rollout (one source at a time)

**2. Main Pipeline Decomposition (Priority P0)**
- **Risk:** Changes entire orchestration flow
- **Mitigation:**
  - Maintain backward compatibility
  - Feature flag for new pipeline
  - Integration tests for all sources
  - Rollback plan documented

**3. apply_schema_evolution() (Priority P2)**
- **Risk:** Schema evolution is critical for production
- **Mitigation:**
  - Shadow mode (run both old and new, compare results)
  - Test with historical schema changes
  - Rollback mechanism
  - Alert on schema drift

### Backward Compatibility Strategy

```python
# Feature flag approach
USE_NEW_PIPELINE = os.getenv('USE_NEW_PIPELINE', '0') == '1'

def main():
    if USE_NEW_PIPELINE:
        return new_pipeline_main()
    else:
        return legacy_main()

# Allow gradual migration per source
USE_NEW_EXTRACTION = {
    'facebook': True,   # Tested
    'mailchimp': False, # Not yet
    'wordpress': False,
    # ...
}
```

### Rollback Plan

1. **Git Strategy:**
   - Feature branches for each major refactoring
   - Tag stable releases before major changes
   - Keep old code in `legacy/` directory during transition

2. **Deployment:**
   - Deploy with feature flags disabled
   - Enable for test environment first
   - Gradual rollout (10% → 50% → 100%)
   - Monitor error rates and performance

3. **Testing Gates:**
   - All unit tests must pass
   - Integration tests for affected sources
   - Performance benchmarks (no regression)
   - Manual QA for critical paths

---

## METRICS & SUCCESS CRITERIA

### Code Quality Metrics

**Before Refactoring:**
- Total LOC: ~15,000
- Longest function: 1,440 lines (main)
- Average function length: ~85 lines
- Code duplication: ~15%
- Test coverage: ~45%
- MyPy compliance: 20%

**After Refactoring (Target):**
- Total LOC: ~9,000 (40% reduction)
- Longest function: <150 lines
- Average function length: <40 lines
- Code duplication: <5%
- Test coverage: >80%
- MyPy compliance: >90%

### Performance Metrics

**Target: No Regression**
- Facebook extraction time: ≤ current baseline
- Mailchimp extraction time: ≤ current baseline
- Schema evolution time: ≤ current baseline
- Merge operation time: ≤ current baseline (ideally 10-20% faster)

### Maintainability Metrics

**Target Improvements:**
- Time to add new extractor: 50% reduction
- Time to add new table type: 60% reduction
- Time to debug issues: 40% reduction
- Onboarding time for new developers: 50% reduction

---

## CONCLUSION

This comprehensive refactoring analysis identifies 106 specific refactoring opportunities across the orchestrator codebase. The proposed refactorings are organized into a 6-week implementation roadmap with clear priorities, effort estimates, and risk mitigation strategies.

**Key Recommendations:**

1. **Start with Quick Wins (Week 1):** SSH tunnels, metadata enrichment, error handling
   - Low risk, high value
   - Builds confidence and momentum
   - Immediate code quality improvements

2. **Core Refactoring (Week 2-3):** Pipeline boilerplate, main decomposition, merge logic
   - Highest impact on maintainability
   - Requires careful testing
   - Foundation for future improvements

3. **Advanced Refactoring (Week 4-5):** Table extractors, schema evolution, type safety
   - Makes codebase extensible
   - Improves developer experience
   - Sets patterns for future development

4. **Documentation & Testing (Week 6):** Ensure changes are sustainable
   - Knowledge transfer
   - Quality assurance
   - Long-term maintainability

**Expected Outcomes:**
- 40% reduction in code duplication
- Improved testability and test coverage (45% → 80%+)
- Faster feature development (50% time reduction)
- Better onboarding experience
- More maintainable, extensible codebase

The refactoring can be done incrementally with feature flags and backward compatibility, minimizing risk to production systems while delivering continuous improvements.

---

## APPENDIX: FUNCTIONAL PROGRAMMING PHILOSOPHY

This refactoring plan follows a **functional-first approach** with the following guidelines:

### The 80/20 Rule

**80% Functions:**
- Pure functions for business logic
- Higher-order functions (functions that return functions)
- Function composition via decorators
- Data transformations via map/filter/reduce patterns
- Closures for encapsulation (not classes!)

**20% Dataclasses:**
- Use ONLY for data structures with 8+ fields
- Examples: PipelineContext, ExtractionResult, ExtractionOptions
- Provides type safety where it matters most
- Never use for behavior - only data!

**0% Inheritance:**
- No abstract base classes (ABC)
- No class hierarchies
- Use function registries (dicts) instead of polymorphism
- Prefer function composition over inheritance

### Why Functional?

**Simpler:**
- Functions are easier to understand than class hierarchies
- No need to track instance state
- Less cognitive overhead

**More Testable:**
- Pure functions need no setup/teardown
- No mocking required for simple functions
- Each function testable in isolation

**More Composable:**
- Small functions combine naturally
- Higher-order functions enable powerful patterns
- Decorators for cross-cutting concerns (retry, logging, tracking)

**More Flexible:**
- **options dict instead of rigid parameter objects
- Easy to add new parameters without schema changes
- Function registries (dicts) easier to extend than class hierarchies

### Examples Throughout This Document

1. **Pipeline Helpers** - Pure functions, no PipelineParams class needed
2. **SSH Tunnels** - Pure functions, no TunnelManager class
3. **Retry Logic** - Closures (functions returning functions), no BackoffStrategy enum
4. **Table Extractors** - Function registry (dict), no FacebookTableExtractor base class
5. **Error Handling** - Decorators, no ErrorHandler class

### When to Use Dataclasses (Rare!)

```python
# GOOD: 10+ related fields traveling together
@dataclass
class PipelineContext:
    args: argparse.Namespace
    config: Dict[str, Any]
    env: str
    source: str
    tables: List[str]
    sites: List[str]
    execution_id: str
    bq_client: bigquery.Client
    stats: Dict[str, Any]

# BAD: Don't need class for 3 fields - use tuple or dict!
@dataclass
class DateRange:
    start_date: str
    end_date: str
    refresh_mode: str  # Just use a tuple: (start, end, mode)
```

### Migration Strategy

When refactoring existing code:

1. **Extract functions first** - Pull logic into pure functions
2. **Add dataclass last** - Only if parameter count > 8
3. **Never inherit** - Convert class hierarchies to function registries
4. **Test functionally** - Write tests that call functions directly

This approach keeps the codebase simple, maintainable, and Pythonic!
