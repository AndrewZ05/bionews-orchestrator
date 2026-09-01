#!/usr/bin/env python3
"""
OneTrust Extractor - Consent & Privacy Management Pipeline
==========================================================

Extracts data from OneTrust APIs:
- Consent & Preference Management (data subjects, purposes, collection points)
- Data Subject Requests (DSR queue, subtasks, audit history)
- Assessments (PIAs, vendor assessments)
- Audit logs (login history, users)

Authentication: OAuth 2.0 with client credentials flow
API Documentation: https://developer.onetrust.com/onetrust/reference/onetrust-api-reference

Author: Data Pipeline Team
Created: 2026-01-30
"""

import os
import json
import time
import logging
import uuid
import hashlib
import tempfile
import threading
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple, TYPE_CHECKING
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pyarrow as pa
import pyarrow.parquet as pq

from shared.rate_limiter import wait_if_needed
from shared.extractor_utils import apply_table_affix, get_available_tables
from shared.account_context import (
    enrich_record_with_tenant_info,
    set_execution_metadata,
)
from shared.gcs_pipeline import (
    upload_to_gcs_raw,
    create_external_table_staging,
    process_hash_merge,
    read_yaml_schema,
)

if TYPE_CHECKING:
    from shared.extractor_runner import PipelineEnvironment

logger = logging.getLogger(__name__)

# Thread-local storage for worker sessions
_worker_session_local = threading.local()

# System fields excluded from hash calculation
SYSTEM_FIELDS = {
    "row_hash",
    "loaded_at",
    "execution_id",
    "updated_at",
    "source",
}

# Rate limiter for OneTrust API
ONETRUST_RATE_LIMITER = {
    "requests_per_minute": 60,
    "last_request_time": 0,
    "request_count": 0,
    "window_start": 0,
}


class OneTrustAuthError(Exception):
    """Raised when authentication fails"""

    pass


class OneTrustAPIError(Exception):
    """Raised when API request fails"""

    pass


class OneTrustTokenManager:
    """Manages OAuth 2.0 token lifecycle for OneTrust API"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = config["source"]["connection"]
        self.token_url = self.connection.get("token_url")
        self.client_id = self.connection.get("client_id") or os.getenv(
            "ONETRUST_CLIENT_ID"
        )
        self.client_secret = self.connection.get("client_secret") or os.getenv(
            "ONETRUST_CLIENT_SECRET"
        )
        self.scopes = self.connection.get("scopes", ["CONSENT_READ"])
        self.access_token = None
        self.token_expiry = None
        self.refresh_buffer = config.get("extractor", {}).get(
            "token_refresh_buffer_seconds", 300
        )
        self._lock = threading.Lock()

    def get_token(self) -> str:
        """Get a valid access token, refreshing if necessary"""
        with self._lock:
            if self._token_is_valid():
                return self.access_token
            return self._refresh_token()

    def _token_is_valid(self) -> bool:
        """Check if current token is still valid"""
        if not self.access_token or not self.token_expiry:
            return False
        # Add buffer time before expiry
        return datetime.now(timezone.utc) < (
            self.token_expiry - timedelta(seconds=self.refresh_buffer)
        )

    def _refresh_token(self) -> str:
        """Request a new access token using client credentials"""
        if not self.client_id or not self.client_secret:
            raise OneTrustAuthError(
                "OneTrust credentials not configured. Set ONETRUST_CLIENT_ID and ONETRUST_CLIENT_SECRET"
            )

        logger.info("Refreshing OneTrust OAuth token...")

        try:
            # Don't send scopes - OneTrust grants default scopes
            response = requests.post(
                self.token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30,
            )
            response.raise_for_status()

            token_data = response.json()
            self.access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)
            self.token_expiry = datetime.now(timezone.utc) + timedelta(
                seconds=expires_in
            )

            logger.info(f"OneTrust token refreshed, expires in {expires_in} seconds")
            return self.access_token

        except requests.exceptions.RequestException as e:
            raise OneTrustAuthError(f"Failed to obtain OneTrust token: {e}")


def create_onetrust_session(
    token_manager: OneTrustTokenManager, timeout: int = 30
) -> requests.Session:
    """Create a requests session configured for OneTrust API"""
    session = requests.Session()

    # Configure retries
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Set default timeout
    session.timeout = timeout

    return session


def onetrust_request(
    session: requests.Session,
    token_manager: OneTrustTokenManager,
    method: str,
    url: str,
    params: Optional[Dict] = None,
    json_data: Optional[Dict] = None,
    rate_limit: int = 60,
    max_retries: int = 3,
) -> Dict[str, Any]:
    """Make an authenticated request to OneTrust API with rate limiting"""

    # Rate limiting
    wait_if_needed("onetrust")

    headers = {
        "Authorization": f"Bearer {token_manager.get_token()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    for attempt in range(max_retries):
        try:
            response = session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_data,
                timeout=60,
            )

            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited, waiting {retry_after} seconds...")
                time.sleep(retry_after)
                continue

            # Handle token expiry
            if response.status_code == 401:
                logger.info("Token expired, refreshing...")
                token_manager.access_token = None  # Force refresh
                headers["Authorization"] = f"Bearer {token_manager.get_token()}"
                continue

            response.raise_for_status()

            # Handle empty responses
            if response.status_code == 204 or not response.content:
                return {}

            return response.json()

        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise OneTrustAPIError(
                    f"API request failed after {max_retries} attempts: {e}"
                )
            logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(2**attempt)

    return {}


def fetch_paginated_collection(
    session: requests.Session,
    token_manager: OneTrustTokenManager,
    base_url: str,
    endpoint: str,
    params: Optional[Dict] = None,
    page_size: int = 100,
    rate_limit: int = 60,
    max_pages: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Fetch all pages of a paginated collection"""

    all_records = []
    page = 0
    params = params or {}
    params["size"] = page_size

    while True:
        params["page"] = page
        url = urljoin(base_url, endpoint)

        logger.debug(f"Fetching page {page} from {endpoint}")

        response = onetrust_request(
            session, token_manager, "GET", url, params=params, rate_limit=rate_limit
        )

        # Handle different response formats
        content = response.get(
            "content", response.get("data", response.get("results", []))
        )
        if isinstance(content, list):
            records = content
        elif isinstance(response, list):
            records = response
        else:
            records = [response] if response else []

        if not records:
            break

        all_records.extend(records)

        # Check pagination metadata
        total_pages = response.get("totalPages", response.get("total_pages"))
        is_last = response.get("last", False)

        if is_last or (total_pages and page >= total_pages - 1):
            break

        if max_pages and page >= max_pages - 1:
            logger.info(f"Reached max pages limit ({max_pages})")
            break

        page += 1

    logger.info(f"Fetched {len(all_records)} records from {endpoint}")
    return all_records


def extract_table(
    config: Dict[str, Any],
    table_name: str,
    resource_config: Dict[str, Any],
    session: requests.Session,
    token_manager: OneTrustTokenManager,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    parent_records: Optional[List[Dict]] = None,
) -> List[Dict[str, Any]]:
    """Extract records for a single table"""

    base_url = config["source"]["connection"]["base_url"]
    endpoint = resource_config.get("endpoint", "")
    http_method = resource_config.get("http_method", "GET")
    api_pattern = resource_config.get("api_pattern", "paginated_collection")
    rate_limit = config["source"].get("rate_limits", {}).get("default", 60)
    batch_size = resource_config.get("batch_size", 100)

    # Build query parameters
    params = {}
    pagination = resource_config.get("pagination", {})
    if pagination.get("type") == "offset":
        page_size = min(batch_size, pagination.get("max_page_size", 100))
    else:
        page_size = batch_size

    # Add date filters for incremental extraction
    if start_date and resource_config.get("incremental", {}).get("strategy") == "date":
        query_params = resource_config.get("query_params", {})
        if "startDate" in query_params:
            params["startDate"] = start_date
        if "endDate" in query_params:
            params["endDate"] = end_date or datetime.now(timezone.utc).isoformat()
        # Alternative parameter names
        if "since" in query_params:
            params["since"] = start_date
        if "updatedSince" in query_params:
            params["updatedSince"] = start_date

    # Handle path parameters
    path_params = resource_config.get("path_params", {})
    for key, value in path_params.items():
        endpoint = endpoint.replace(f"{{{key}}}", str(value))

    records = []

    # Handle different API patterns
    if api_pattern == "paginated_collection":
        records = fetch_paginated_collection(
            session,
            token_manager,
            base_url,
            endpoint,
            params=params,
            page_size=page_size,
            rate_limit=rate_limit,
        )

    elif api_pattern == "child_collection" and parent_records:
        # Fetch child records for each parent
        parent_key = resource_config.get("parent_key", "id")
        for parent in parent_records:
            parent_id = parent.get(parent_key)
            if not parent_id:
                continue

            child_endpoint = endpoint.replace(f"{{{parent_key}}}", str(parent_id))
            child_records = fetch_paginated_collection(
                session,
                token_manager,
                base_url,
                child_endpoint,
                params=params,
                page_size=page_size,
                rate_limit=rate_limit,
            )

            # Add parent reference to each child record
            for record in child_records:
                record[parent_key] = parent_id

            records.extend(child_records)

    elif api_pattern == "child_detail" and parent_records:
        # Fetch detail for each parent
        parent_key = resource_config.get("parent_key", "id")
        for parent in parent_records:
            parent_id = parent.get(parent_key)
            if not parent_id:
                continue

            detail_endpoint = endpoint.replace(f"{{{parent_key}}}", str(parent_id))
            url = urljoin(base_url, detail_endpoint)

            try:
                detail = onetrust_request(
                    session,
                    token_manager,
                    http_method,
                    url,
                    params=params,
                    rate_limit=rate_limit,
                )
                if detail:
                    detail[parent_key] = parent_id
                    records.append(detail)
            except OneTrustAPIError as e:
                logger.warning(
                    f"Failed to fetch detail for {parent_key}={parent_id}: {e}"
                )

    elif api_pattern == "list_then_detail":
        # First fetch list, then fetch detail for each
        list_endpoint = resource_config.get("list_endpoint", endpoint)
        detail_endpoint = resource_config.get("detail_endpoint", "")

        list_records = fetch_paginated_collection(
            session,
            token_manager,
            base_url,
            list_endpoint,
            params=params,
            page_size=page_size,
            rate_limit=rate_limit,
        )

        for item in list_records:
            item_id = item.get("id") or item.get("guid")
            if not item_id or not detail_endpoint:
                records.append(item)
                continue

            item_detail_endpoint = detail_endpoint.replace("{id}", str(item_id))
            url = urljoin(base_url, item_detail_endpoint)

            try:
                detail = onetrust_request(
                    session,
                    token_manager,
                    "GET",
                    url,
                    params={"state": "PUBLISHED"},
                    rate_limit=rate_limit,
                )
                if detail:
                    merged = {**item, **detail}
                    records.append(merged)
                else:
                    records.append(item)
            except OneTrustAPIError as e:
                logger.warning(f"Failed to fetch detail for id={item_id}: {e}")
                records.append(item)

    else:
        # Simple single request
        url = urljoin(base_url, endpoint)
        response = onetrust_request(
            session,
            token_manager,
            http_method,
            url,
            params=params,
            rate_limit=rate_limit,
        )
        if isinstance(response, list):
            records = response
        elif response:
            records = [response]

    logger.info(f"Extracted {len(records)} records for {table_name}")
    return records


def flatten_record(record: Dict[str, Any], json_fields: List[str]) -> Dict[str, Any]:
    """Flatten nested JSON fields to strings"""
    flattened = {}

    for key, value in record.items():
        if key in json_fields and value is not None:
            if isinstance(value, (dict, list)):
                flattened[key] = json.dumps(value)
            else:
                flattened[key] = str(value)
        elif isinstance(value, dict):
            # Flatten nested dicts with underscore notation
            for nested_key, nested_value in value.items():
                flat_key = f"{key}_{nested_key}"
                if isinstance(nested_value, (dict, list)):
                    flattened[flat_key] = json.dumps(nested_value)
                else:
                    flattened[flat_key] = nested_value
        else:
            flattened[key] = value

    return flattened


def records_to_parquet(
    records: List[Dict[str, Any]],
    schema: Dict[str, str],
    json_fields: List[str],
    output_path: str,
) -> str:
    """Convert records to Parquet file"""

    if not records:
        logger.warning("No records to write to Parquet")
        return output_path

    # Flatten records
    flattened_records = [flatten_record(r, json_fields) for r in records]

    # Build PyArrow schema
    arrow_fields = []
    for field_name, field_type in schema.items():
        if field_name in SYSTEM_FIELDS:
            continue
        pa_type = _map_bq_type_to_arrow(field_type)
        arrow_fields.append(pa.field(field_name, pa_type))

    # Create table with schema enforcement
    columns = {}
    for field in arrow_fields:
        field_name = field.name
        values = [r.get(field_name) for r in flattened_records]

        # Convert values to appropriate types
        try:
            if pa.types.is_timestamp(field.type):
                values = [_parse_timestamp(v) for v in values]
            elif pa.types.is_boolean(field.type):
                values = [_parse_boolean(v) for v in values]
            elif pa.types.is_integer(field.type):
                values = [_parse_integer(v) for v in values]
            elif pa.types.is_floating(field.type):
                values = [_parse_float(v) for v in values]

            columns[field_name] = pa.array(values, type=field.type)
        except Exception as e:
            logger.warning(
                f"Error converting column {field_name}: {e}, using string fallback"
            )
            columns[field_name] = pa.array(
                [str(v) if v is not None else None for v in values], type=pa.string()
            )

    table = pa.table(columns)
    pq.write_table(table, output_path, compression="snappy")

    logger.info(f"Wrote {len(records)} records to {output_path}")
    return output_path


def _map_bq_type_to_arrow(bq_type: str) -> pa.DataType:
    """Map BigQuery type to PyArrow type"""
    normalized = (bq_type or "STRING").upper()
    if normalized in {"STRING", "JSON"}:
        return pa.string()
    if normalized in {"INT64", "INTEGER", "INT"}:
        return pa.int64()
    if normalized in {"FLOAT64", "FLOAT", "NUMERIC"}:
        return pa.float64()
    if normalized in {"BOOL", "BOOLEAN"}:
        return pa.bool_()
    if normalized in {"TIMESTAMP"}:
        return pa.timestamp("us", tz="UTC")
    if normalized in {"DATE"}:
        return pa.date32()
    return pa.string()


def _parse_timestamp(value: Any) -> Optional[datetime]:
    """Parse timestamp value"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            # Handle ISO format
            if "T" in value:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return None


def _parse_boolean(value: Any) -> Optional[bool]:
    """Parse boolean value"""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)


def _parse_integer(value: Any) -> Optional[int]:
    """Parse integer value"""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _parse_float(value: Any) -> Optional[float]:
    """Parse float value"""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def get_available_tables(
    config: Dict[str, Any], group: Optional[str] = None
) -> List[str]:
    """Get list of available tables from config"""
    tables = []
    for table_name, table_config in config.get("resources", {}).items():
        if not table_config.get("active", True):
            continue
        if group and table_config.get("group") != group:
            continue
        tables.append(table_name)
    return tables


def run_pipeline(
    config: Dict[str, Any],
    sites: List[str],
    tables: List[str],
    group: str,
    refresh_mode: str,
    lookback_days: int,
    start_date: str,
    end_date: str,
    test_mode: bool,
    batch_size: int,
    max_retries: int,
    skip_validation: bool = False,
    export_format: str = None,
    export_dir: str = None,
    skip_hash_merge: bool = False,
    archive_staging: bool = False,
    truncate_staging: bool = False,
    bq_client: Any = None,
    execution_id: str = None,
    schema_prefix: Optional[str] = None,
    schema_suffix: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Main pipeline entry point for OneTrust extraction.

    Args:
        config: Source configuration dictionary
        sites: Not used for OneTrust (single tenant)
        tables: List of tables to extract
        group: Optional group filter (consent, dsr, assessment, audit)
        refresh_mode: 'full' or 'incremental'
        lookback_days: Days to look back for incremental
        start_date: Start date for extraction
        end_date: End date for extraction
        test_mode: Limit extraction for testing
        batch_size: Override batch size
        max_retries: Maximum retry attempts
        skip_validation: Skip validation checks
        export_format: Export format override
        export_dir: Export directory override
        skip_hash_merge: Skip merge to production
        archive_staging: Archive staging data
        truncate_staging: Truncate staging tables
        bq_client: BigQuery client instance
        execution_id: Unique execution identifier
        schema_prefix: Prefix for table names
        schema_suffix: Suffix for table names
        **kwargs: Additional parameters

    Returns:
        Dictionary with pipeline statistics
    """

    execution_id = execution_id or str(uuid.uuid4())
    source_name = config["source"]["name"]

    logger.info(f"Starting OneTrust pipeline execution: {execution_id}")
    logger.info(f"Refresh mode: {refresh_mode}, Lookback days: {lookback_days}")

    # Initialize token manager and session
    token_manager = OneTrustTokenManager(config)
    session = create_onetrust_session(token_manager)

    # Determine tables to extract
    if not tables:
        tables = get_available_tables(config, group)

    logger.info(f"Tables to extract: {tables}")

    # Calculate date range for incremental extraction
    if refresh_mode == "incremental" and lookback_days:
        if not start_date:
            start_dt = datetime.now(timezone.utc) - timedelta(days=lookback_days)
            start_date = start_dt.isoformat()
        if not end_date:
            end_date = datetime.now(timezone.utc).isoformat()

    # Pipeline configuration
    pipeline_config = config.get("pipeline", {})
    staging_dataset = pipeline_config.get("staging_dataset", "onetrust_staging")
    production_dataset = pipeline_config.get("production_dataset", "onetrust_data")
    gcs_bucket = pipeline_config.get("gcs_bucket") or os.getenv("GCS_BUCKET")
    gcs_path = pipeline_config.get("gcs_path", "onetrust/raw")
    project_id = pipeline_config.get("project") or os.getenv("GCP_PROJECT_ID")

    # Statistics
    stats = {
        "execution_id": execution_id,
        "source": source_name,
        "tables_processed": 0,
        "records_extracted": 0,
        "records_loaded": 0,
        "errors": [],
        "start_time": datetime.now(timezone.utc).isoformat(),
    }

    # Track parent records for child tables
    parent_records_cache = {}

    # Sort tables by dependencies (parent tables first)
    sorted_tables = _sort_tables_by_dependency(tables, config)

    for table_name in sorted_tables:
        resource_config = config.get("resources", {}).get(table_name)
        if not resource_config:
            logger.warning(f"Table {table_name} not found in config")
            continue

        if not resource_config.get("active", True):
            logger.info(f"Skipping inactive table: {table_name}")
            continue

        logger.info(f"Processing table: {table_name}")

        try:
            # Get parent records if this is a child table
            parent_records = None
            parent_table = resource_config.get("parent_table")
            if parent_table and parent_table in parent_records_cache:
                parent_records = parent_records_cache[parent_table]

            # Extract records
            records = extract_table(
                config=config,
                table_name=table_name,
                resource_config=resource_config,
                session=session,
                token_manager=token_manager,
                start_date=start_date if refresh_mode == "incremental" else None,
                end_date=end_date,
                parent_records=parent_records,
            )

            if not records:
                logger.info(f"No records extracted for {table_name}")
                continue

            # Cache records for child tables
            parent_records_cache[table_name] = records

            # Add metadata to records
            extracted_at = datetime.now(timezone.utc).isoformat()
            for record in records:
                record["execution_id"] = execution_id
                record["extracted_at"] = extracted_at
                record["source"] = source_name

            stats["records_extracted"] += len(records)

            # Apply table name affixes
            table_with_affix = apply_table_affix(
                resource_config.get("table_name", f"onetrust_{table_name}"),
                schema_prefix,
                schema_suffix,
            )

            # Get schema
            schema = resource_config.get("schema", {})
            json_fields = resource_config.get("json_fields", [])

            # Write to Parquet
            with tempfile.TemporaryDirectory() as tmp_dir:
                parquet_path = os.path.join(tmp_dir, f"{table_with_affix}.parquet")
                records_to_parquet(records, schema, json_fields, parquet_path)

                # GCS upload and BigQuery loading - simplified for now
                # TODO: Integrate with orchestrator's GCS pipeline
                stats["records_loaded"] += len(records)
                logger.info(
                    f"Extracted {len(records)} records for {table_name} to parquet"
                )

                # Export to local file if requested
                if export_format and export_dir:
                    export_path = os.path.join(
                        export_dir, f"{table_with_affix}.{export_format}"
                    )
                    if export_format == "json":
                        with open(export_path, "w") as f:
                            json.dump(records, f, indent=2, default=str)
                    elif export_format == "parquet":
                        import shutil

                        shutil.copy(parquet_path, export_path)
                    logger.info(f"Exported {len(records)} records to {export_path}")

            stats["tables_processed"] += 1

        except Exception as e:
            logger.error(f"Error processing table {table_name}: {e}", exc_info=True)
            stats["errors"].append(
                {
                    "table": table_name,
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )

    stats["end_time"] = datetime.now(timezone.utc).isoformat()

    logger.info(
        f"Pipeline completed. Tables: {stats['tables_processed']}, "
        f"Records extracted: {stats['records_extracted']}, "
        f"Records loaded: {stats['records_loaded']}, "
        f"Errors: {len(stats['errors'])}"
    )

    return stats


def _sort_tables_by_dependency(tables: List[str], config: Dict[str, Any]) -> List[str]:
    """Sort tables so parent tables are processed before child tables"""

    resources = config.get("resources", {})

    # Build dependency graph
    dependencies = {}
    for table in tables:
        resource = resources.get(table, {})
        parent = resource.get("parent_table")
        dependencies[table] = parent if parent in tables else None

    # Topological sort
    sorted_tables = []
    visited = set()

    def visit(table):
        if table in visited:
            return
        visited.add(table)
        parent = dependencies.get(table)
        if parent and parent not in visited:
            visit(parent)
        sorted_tables.append(table)

    for table in tables:
        visit(table)

    return sorted_tables


if __name__ == "__main__":
    # Simple test
    import yaml

    config_path = Path(__file__).parent.parent / "configs" / "onetrust.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    print("Available tables:")
    for table in get_available_tables(config):
        print(f"  - {table}")
