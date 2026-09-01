#!/usr/bin/env python3
# Facebook-specific extraction with ASYNC-first approach

import os
import sys
import time
import json
import logging
from typing import Dict, List, Any, Iterator, Optional, Tuple
from datetime import datetime, timedelta, date, timezone
from pathlib import Path
import pandas as pd
from google.cloud import bigquery

# Import centralized timestamp utilities
from shared.timestamp_utils import parse_facebook_timestamp
from shared.extractor_utils import (
    apply_table_affix,
    get_available_tables,
    sanitize_bq_column_name,
)
from shared.account_context import set_execution_metadata
from shared.extractor_runner import initialize_pipeline_environment
from shared.facebook_client import (
    RESOURCE_CONFIG,
    extract_insights_async as facebook_extract_insights_async,
    fetch_resource_batch,
    wait_for_async_jobs as facebook_wait_for_async_jobs,
    wait_for_facebook,
)

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.page import Page
from facebook_business.adobjects.user import User
from facebook_business.adobjects.leadgenform import LeadgenForm
from facebook_business.exceptions import FacebookRequestError

# Import enhanced rate limiter
from shared.rate_limiter import (
    with_rate_limit,
    handle_rate_limit_error,
    reset_rate_limit,
    wait_if_needed,
)

# Import data processing utilities
from shared.data_processor import ensure_string_ids

# Import GCS pipeline for WordPress pattern
from shared.gcs_pipeline import extract_to_local_parquet

# Set up logging
logger = logging.getLogger(__name__)

# Import post spool pattern for eliminating duplicate post fetches
from plugins.facebook_post_spool import create_spool, get_current_spool, reset_spool


# Per-table terminal status values, recorded in pipeline_stats['table_status'].
# These are the authoritative verdict the orchestrator consumes -- success and
# zero_rows count as healthy; rate_limited is recoverable (next incremental run
# backfills); only `failed` is a hard failure that should alert.
TABLE_STATUS_SUCCESS = "success"
TABLE_STATUS_ZERO_ROWS = "zero_rows"
TABLE_STATUS_RATE_LIMITED = "rate_limited"
TABLE_STATUS_FAILED = "failed"

# Rate-limit classification lives in shared/fb_error_classify (SDK-free + unit
# tested). _RATE_LIMIT_CODES/_RATE_LIMIT_TEXT re-exported for the inline call
# site below and any existing imports.
from shared.fb_error_classify import RATE_LIMIT_CODES as _RATE_LIMIT_CODES
from shared.fb_error_classify import RATE_LIMIT_TEXT as _RATE_LIMIT_TEXT
from shared.fb_error_classify import (
    classify_rate_limit,
    is_facebook_rate_limit,
)  # noqa: F401


def wait_for_async_jobs(
    jobs: List[Dict[str, Any]], max_wait_minutes: int = 30
) -> List[Dict[str, Any]]:
    return facebook_wait_for_async_jobs(jobs, max_wait_minutes=max_wait_minutes)


def extract_insights_async(
    account_id: str,
    table_config: Dict[str, Any],
    date_range: Tuple[date, date],
    level: str = "ad",
    test_mode: bool = False,
) -> Iterator[Dict[str, Any]]:
    return facebook_extract_insights_async(
        account_id,
        table_config,
        date_range,
        level=level,
        test_mode=test_mode,
    )


# Timestamp parsing moved to shared/timestamp_utils.py for centralized handling


def initialize_api(config: Dict[str, Any]) -> bool:
    """Initialize Facebook API"""
    try:
        access_token = config.get("source", {}).get("connection", {}).get(
            "access_token"
        ) or os.getenv("FACEBOOK_ACCESS_TOKEN")
        app_id = os.getenv("FACEBOOK_APP_ID")
        app_secret = os.getenv("FACEBOOK_APP_SECRET")

        if not access_token:
            logger.error("Facebook access token not found")
            return False

        FacebookAdsApi.init(app_id, app_secret, access_token, api_version="v25.0")
        logger.info("Facebook API initialized successfully (Graph API v25.0)")
        return True

    except Exception as e:
        logger.error(f"Failed to initialize Facebook API: {e}")
        return False


def _canonical_account_id(accounts: List[str] = None) -> str:
    """
    Return the canonical Facebook account_id for data attribution.

    Always prefers FACEBOOK_ACCOUNT_ID from environment over the runtime
    `accounts` list. This prevents page IDs (passed via --sites for retries)
    from being mislabeled as account_ids in BigQuery.

    Falls back to accounts[0] only when env var is unset AND the value looks
    like a real Facebook account ID (numeric, no 'act_' prefix is OK).
    """
    env_account_id = os.getenv("FACEBOOK_ACCOUNT_ID")
    if env_account_id:
        return env_account_id
    if accounts:
        # Strip 'act_' prefix if present for consistent storage
        first = accounts[0]
        if isinstance(first, str) and first.startswith("act_"):
            first = first[4:]
        return first
    return "unknown"


def get_available_sites(config: Dict[str, Any]) -> List[str]:
    """Get available Facebook accounts"""
    if not initialize_api(config):
        return []

    try:
        user = User(fbid="me")
        accounts = user.get_ad_accounts(fields=["id", "name", "account_status"])
        account_list = []
        for account in accounts:
            account_id = account["id"]
            if not account_id.startswith("act_"):
                account_id = f"act_{account_id}"
            account_list.append(account_id)
        logger.info(f"Retrieved {len(account_list)} accounts from Facebook API")
        return account_list
    except Exception as e:
        logger.error(f"Error getting accounts: {e}")
        return []


def serialize_facebook_object(obj: Any) -> Any:
    """Convert Facebook SDK objects to JSON-serializable format recursively"""
    if obj is None:
        return None

    # If it's already a basic type, return it
    if isinstance(obj, (str, int, float, bool)):
        return obj

    # If it's a list, serialize each item
    if isinstance(obj, list):
        return [serialize_facebook_object(item) for item in obj]

    # If it's a dict, serialize each value
    if isinstance(obj, dict):
        return {k: serialize_facebook_object(v) for k, v in obj.items()}

    # Check if it's a Facebook SDK object by class name
    class_name = obj.__class__.__name__
    facebook_classes = [
        "Targeting",
        "TargetingGeoLocation",
        "TargetingGeoLocationZip",
        "TargetingGeoLocationMarket",
        "TargetingGeoLocationRegion",
        "TargetingGeoLocationCity",
        "TargetingGeoLocationCountry",
        "AdCreative",
        "AdCreativeObjectStorySpec",
        "AdCreativeLinkData",
        "AdAccount",
        "Campaign",
        "AdSet",
        "Ad",
    ]

    # If it's a Facebook SDK object, extract the data
    if any(fb_class in class_name for fb_class in facebook_classes) or hasattr(
        obj, "_data"
    ):
        if hasattr(obj, "_data"):
            # Recursively serialize the _data content
            return serialize_facebook_object(obj._data)
        elif hasattr(obj, "export_all_data"):
            # Recursively serialize the exported data
            return serialize_facebook_object(obj.export_all_data())
        elif hasattr(obj, "export_value"):
            # Recursively serialize the exported value
            return serialize_facebook_object(obj.export_value())
        elif hasattr(obj, "__dict__"):
            # Try to serialize the object's dictionary
            return serialize_facebook_object(vars(obj))

    # For datetime objects - keep as datetime for Parquet (don't convert to string)
    # Parquet will handle datetime serialization properly
    if hasattr(obj, "isoformat"):
        # Return datetime as-is for Parquet to handle
        # Parquet schema enforcement will ensure proper precision
        return obj

    # Fallback to string representation
    return str(obj)


def extract_hierarchy(
    accounts: List[str],
    resource_name: str,
    test_mode: bool,
    date_range: tuple = None,
    rebuild_mode: bool = False,
    table_config: Dict[str, Any] = None,
) -> Iterator[Dict]:
    """Extract campaigns, adsets, ads, or adcreatives with rate limit handling

    Args:
        accounts: List of Facebook account IDs
        resource_name: Type of resource (campaigns, adsets, ads)
        test_mode: If True, limit to 10 records
        date_range: Optional (start_date, end_date) for incremental filtering
        rebuild_mode: If True, ignore date_range and extract ALL records
        table_config: Optional table config dict to read incremental fields from YAML
    """
    if resource_name not in RESOURCE_CONFIG:
        logger.error(f"Unknown resource type: {resource_name}")
        return

    res_config = RESOURCE_CONFIG[resource_name]

    for account_id in accounts:
        if not account_id.startswith("act_"):
            account_id = f"act_{account_id}"

        try:
            count = 0
            retry_count = 0
            max_retries = 3

            # Use params to control pagination
            params = {"limit": 100}  # Smaller batch size to avoid rate limits

            # Get date fields from YAML config (or use defaults)
            date_fields = ["updated_time", "created_time"]  # Default fallback
            inc = (table_config or {}).get("incremental", {})
            if inc.get("fields"):
                date_fields = inc["fields"]
                logger.debug(
                    f"Using YAML-configured date fields for {resource_name}: {date_fields}"
                )

            # Date filtering for incremental loads (skip if rebuild_mode)
            filter_start_dt = None
            if date_range and not rebuild_mode:
                start_date, end_date = date_range
                # Convert date objects to datetime if needed
                if isinstance(start_date, date) and not isinstance(
                    start_date, datetime
                ):
                    filter_start_dt = datetime.combine(start_date, datetime.min.time())
                else:
                    filter_start_dt = start_date
                logger.info(
                    f"Incremental mode: filtering records by {' or '.join(date_fields)} >= {filter_start_dt}"
                )
            elif rebuild_mode:
                logger.debug(
                    f"Rebuild mode: extracting ALL {resource_name} (no date filtering)"
                )

            while True:
                try:
                    # Get a batch of items using cached function
                    items = fetch_resource_batch(
                        account_id=account_id,
                        resource_name=resource_name,
                        fields=res_config["fields"],
                        params=params,
                    )

                    for item in items:
                        # Add account_id and extraction time
                        record = item.copy()
                        record["account_id"] = account_id
                        record["extracted_at"] = (
                            datetime.now()
                        )  # Keep as datetime for Parquet

                        # Convert timestamp fields to datetime objects for proper Parquet TIMESTAMP type
                        # This ensures timestamps are stored as TIMESTAMP in BigQuery, not STRING
                        # Includes all common timestamp fields across Facebook API (hierarchy and insights tables)
                        # Handles both string timestamps (ISO format) and Unix timestamps (integers)
                        timestamp_fields = [
                            "created_time",
                            "updated_time",
                            "start_time",
                            "stop_time",
                            "end_time",
                            "date_start",
                            "date_stop",
                            "date_created",
                            "date_updated",
                            "date_sent",
                            "date_recorded",
                            "date_modified",
                            "date_notified",
                            "last_updated",
                            "time_created",
                            "time_updated",
                        ]  # Unix timestamps for customaudiences
                        for ts_field in timestamp_fields:
                            if ts_field in record and record[ts_field]:
                                try:
                                    # Parse timestamp using centralized utility
                                    record[ts_field] = parse_facebook_timestamp(
                                        record[ts_field]
                                    )
                                except Exception as e:
                                    logger.debug(
                                        f"Could not parse {ts_field} '{record[ts_field]}': {e}"
                                    )
                                    record[ts_field] = None

                        # Client-side date filtering for incremental loads (skip if rebuild_mode)
                        if filter_start_dt:
                            # Check date fields in priority order (from YAML config)
                            record_date_value = None
                            for field in date_fields:
                                if record.get(field):
                                    record_date_value = record.get(field)
                                    break

                            if record_date_value:
                                try:
                                    from dateutil import parser

                                    # Check if it's already a datetime object (after our conversion above)
                                    if isinstance(record_date_value, datetime):
                                        record_date = record_date_value
                                    else:
                                        record_date = parser.parse(record_date_value)
                                    # Make timezone-aware comparison if needed
                                    if (
                                        record_date.tzinfo is None
                                        and filter_start_dt.tzinfo
                                    ):
                                        record_date = record_date.replace(
                                            tzinfo=filter_start_dt.tzinfo
                                        )
                                    elif (
                                        filter_start_dt.tzinfo is None
                                        and record_date.tzinfo
                                    ):
                                        filter_start_dt_aware = filter_start_dt.replace(
                                            tzinfo=record_date.tzinfo
                                        )
                                    else:
                                        filter_start_dt_aware = filter_start_dt

                                    # Skip records not updated/created since start date
                                    if record_date < filter_start_dt_aware:
                                        continue
                                except Exception as e:
                                    # If we can't parse the date, include the record to be safe
                                    logger.debug(
                                        f"Could not parse date '{record_date_value}': {e}"
                                    )

                        # EXTRACT CREATIVE BEFORE SERIALIZATION (for ads table)
                        creative_obj = None
                        if resource_name == "ads" and "creative" in record:
                            creative_value = record.get("creative")
                            if creative_value:
                                creative_obj = serialize_facebook_object(creative_value)
                                # Store for later extraction
                                record["_creative_obj"] = creative_obj

                        # Process all fields to handle Facebook SDK objects
                        for field, value in list(record.items()):
                            if value is None:
                                continue

                            # Fields that should be serialized as JSON
                            json_fields = [
                                "targeting",
                                "creative",
                                "object_story_spec",
                                "custom_audiences",
                                "excluded_custom_audiences",
                            ]

                            if field in json_fields:
                                serialized = serialize_facebook_object(value)
                                record[field] = (
                                    json.dumps(serialized) if serialized else None
                                )

                                if field == "creative" and isinstance(serialized, dict):
                                    record["creative_id"] = serialized.get("id")

                            elif not isinstance(
                                value, (str, int, float, bool, list, dict, type(None))
                            ):
                                serialized = serialize_facebook_object(value)
                                if isinstance(serialized, (dict, list)):
                                    record[field] = json.dumps(serialized)
                                else:
                                    record[field] = serialized

                        yield record

                        count += 1
                        if test_mode and count >= 10:
                            break

                    # Handle pagination: continue if we got a full page (100+ items) and have a cursor
                    if len(items) >= 100:
                        # Add delay to avoid rate limits
                        time.sleep(0.5)
                        # Try to get next page cursor from items iterator (if it has __paging__)
                        try:
                            if hasattr(items, "__paging__"):
                                next_cursor = items.__paging__.get("after")
                                if next_cursor:
                                    params["after"] = next_cursor
                                    logger.debug(
                                        f"Fetching next page with cursor: {next_cursor[:20]}..."
                                    )
                                    continue  # Fetch next page
                                else:
                                    logger.debug(f"No more pages available (no cursor)")
                                    break
                            else:
                                # No paging info available, stop pagination
                                logger.debug(
                                    f"Items object has no __paging__ attribute, stopping pagination"
                                )
                                break
                        except Exception as e:
                            logger.warning(
                                f"Error getting next page cursor: {e}, stopping pagination"
                            )
                            break
                    else:
                        # Got fewer than 100 items, this is the last page
                        logger.debug(
                            f"Got {len(items)} items (< 100), end of pagination"
                        )
                        break

                except FacebookRequestError as e:
                    error_code = e.api_error_code()
                    error_msg = str(e).lower()

                    # Rate-limit classification (4=App, 17=User, 80004=Account).
                    # Structured code is authoritative; text is a fallback only
                    # when no code is present (see shared/fb_error_classify).
                    is_rate_limit = classify_rate_limit(error_code, error_msg)

                    if is_rate_limit:
                        retry_count += 1

                        # Different backoff strategies based on error code
                        if error_code == 80004:
                            # Account-level rate limit - can take up to 1 hour
                            # Use aggressive backoff: 5min, 15min, 30min, 60min
                            wait_times = [300, 900, 1800, 3600]
                            max_retries = 4
                        elif error_code == 17:
                            # User request limit - typically 10-30 minutes
                            wait_times = [180, 600, 1200]
                            max_retries = 3
                        else:
                            # App rate limit or text-based detection - shorter backoff
                            wait_times = [60, 180, 300]
                            max_retries = 3

                        if retry_count <= max_retries:
                            wait_time = wait_times[
                                min(retry_count - 1, len(wait_times) - 1)
                            ]
                            wait_minutes = wait_time / 60
                            logger.warning(
                                f"Rate limit hit for {account_id} (error code {error_code}). Waiting {wait_minutes:.1f} minutes before retry {retry_count}/{max_retries}"
                            )
                            logger.info(
                                f"  Facebook rate limit message: {str(e)[:200]}"
                            )
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error(
                                f"Max retries ({max_retries}) reached for {account_id}. Extracted {count} {resource_name} before rate limit"
                            )
                            logger.error(
                                f"  Recommendation: Wait 1 hour and re-run, or run at a different time of day"
                            )
                            break
                    else:
                        # Retry transient server errors (HTTP 500, error code 1/2)
                        is_transient = (
                            e.http_status() == 500
                            or error_code in [1, 2]
                            or "unknown error" in error_msg
                        )
                        if is_transient and retry_count < 3:
                            retry_count += 1
                            wait_secs = 30 * retry_count
                            logger.warning(
                                f"Transient Facebook API error for {account_id} (HTTP {e.http_status()}, code {error_code}). "
                                f"Retrying in {wait_secs}s (attempt {retry_count}/3)"
                            )
                            time.sleep(wait_secs)
                            continue
                        else:
                            logger.error(
                                f"Facebook API error for {account_id} (code {error_code}): {e}"
                            )
                            break

                except Exception as e:
                    import traceback

                    logger.error(f"Unexpected error: {e}")
                    logger.error(f"Error type: {type(e).__name__}")
                    logger.error(f"Traceback: {traceback.format_exc()}")
                    break

            if count > 0:
                logger.info(f"Extracted {count} {resource_name} for {account_id}")

        except Exception as e:
            logger.error(f"Failed to initialize extraction for {account_id}: {e}")
            continue


def _legacy_wait_for_async_jobs(
    jobs: List[Dict], max_wait_minutes: int = 30
) -> List[Dict]:
    """Wait for async jobs to complete with adaptive polling and progress tracking"""
    from shared.adaptive_polling import conservative_sleep

    start_time = time.time()
    max_wait_seconds = max_wait_minutes * 60

    logger.info(f"Waiting for {len(jobs)} async jobs to complete...")
    logger.info("This may take 5-15 minutes depending on data size...")

    # CRITICAL: Wait 5 seconds before first check to allow Facebook to process job creation
    # Checking immediately after creation causes "Call was not successful" errors
    logger.debug("Waiting 5 seconds for Facebook to process job creation...")
    time.sleep(5)

    poll_iteration = 0
    while True:
        all_complete = True
        jobs_complete = 0

        for job_info in jobs:
            if job_info.get("status") in ["Job Completed", "Job Failed"]:
                jobs_complete += 1
                continue

            try:
                # Use the stored async_job object which has proper account context
                # DO NOT recreate AdReportRun from string ID - causes truncation
                async_job_obj = job_info["async_job"]

                # Get job status AND error messages if available
                # CRITICAL: api_get() returns a dict, don't overwrite the object!
                # NOTE: Removed 'report_run_id' - not supported in AdReportRunV2 (Facebook API v25.0+)
                job_status_data = async_job_obj.api_get(
                    fields=["async_status", "async_percent_completion", "id"]
                )

                status = job_status_data.get("async_status", "Unknown")
                percent = job_status_data.get("async_percent_completion", 0)

                job_info["status"] = status
                job_info["percent_complete"] = percent

                if status == "Job Completed":
                    logger.info(f"  Job {job_info.get('chunk_num', '?')} completed!")
                    jobs_complete += 1
                elif status == "Job Failed":
                    # Get error details from the job status response
                    # Facebook may include error info in the response or we need to fetch it
                    error_msg = job_status_data.get(
                        "error_message",
                        job_status_data.get("async_error", "Unknown error"),
                    )

                    # Try to get full job details for error information
                    try:
                        full_job_data = async_job_obj.api_get()
                        error_details = str(full_job_data)[:200]
                        logger.error(
                            f"  Job {job_info.get('chunk_num', '?')} FAILED: {error_msg}"
                        )
                        logger.error(f"    Job ID: {job_info['job_id']}")
                        logger.error(
                            f"    Date range: {job_info.get('date_start', '?')} to {job_info.get('date_end', '?')}"
                        )
                        logger.error(f"    Full details: {error_details}")
                    except:
                        logger.error(
                            f"  Job {job_info.get('chunk_num', '?')} FAILED: {error_msg}"
                        )
                        logger.error(f"    Job ID: {job_info['job_id']}")
                        logger.error(
                            f"    Date range: {job_info.get('date_start', '?')} to {job_info.get('date_end', '?')}"
                        )

                    job_info["error"] = str(error_msg)
                    jobs_complete += 1
                else:
                    all_complete = False

            except Exception as e:
                # Log full error details for failed job status checks
                logger.error(
                    f"  Job {job_info.get('chunk_num', '?')} status check FAILED:"
                )
                logger.error(f"    Job ID: {job_info['job_id']}")
                logger.error(
                    f"    Date range: {job_info.get('date_start', '?')} to {job_info.get('date_end', '?')}"
                )
                logger.error(f"    Error: {str(e)[:500]}")
                logger.debug(f"    Full error details:\n{str(e)}")

                job_info["status"] = "Job Failed"
                job_info["error"] = str(e)[:500]  # Increased from 100 to 500 chars
                jobs_complete += 1

        # Progress update
        elapsed_minutes = (time.time() - start_time) / 60
        logger.info(
            f"Jobs complete: {jobs_complete}/{len(jobs)} | Elapsed: {elapsed_minutes:.1f} min"
        )

        if all_complete:
            break

        if (time.time() - start_time) > max_wait_seconds:
            logger.warning(f"Timeout after {max_wait_minutes} minutes")
            break

        # Adaptive polling: starts at 10s, increases to 30s max
        # More responsive than fixed 30s, but still conservative
        conservative_sleep(
            base_seconds=10.0,
            iteration=poll_iteration,
            max_seconds=30.0,
            add_jitter=True,
        )
        poll_iteration += 1

    return jobs


def _legacy_extract_insights_async(
    account_id: str,
    table_config: Dict[str, Any],
    date_range: Tuple[date, date],
    level: str = "ad",
    test_mode: bool = False,
) -> Iterator[Dict]:
    """Universal ASYNC insights extraction"""

    if not account_id.startswith("act_"):
        account_id = f"act_{account_id}"

    logger.info(f"Starting ASYNC {level}_insights extraction for {account_id}")
    logger.info(f"Date range: {date_range[0]} to {date_range[1]}")

    # Calculate total days and determine chunk strategy
    total_days = (date_range[1] - date_range[0]).days

    # Smart chunking based on data size and level
    if level == "account":
        chunk_days = 180 if total_days > 365 else total_days
    elif total_days > 365:
        chunk_days = 90  # 3-month chunks for large ranges
    elif total_days > 90:
        chunk_days = 30  # Monthly chunks
    else:
        chunk_days = total_days  # Single chunk for small ranges

    total_chunks = (total_days + chunk_days - 1) // chunk_days
    logger.info(f"Using {chunk_days}-day chunks ({total_chunks} total chunks)")

    current_date = date_range[0]
    chunk_count = 0
    all_jobs = []

    # Step 1: Create all async jobs
    logger.info("\n STEP 1: Creating async export jobs...")
    while current_date < date_range[1]:
        chunk_end = min(current_date + timedelta(days=chunk_days - 1), date_range[1])
        chunk_count += 1

        logger.info(
            f"Creating job {chunk_count}/{total_chunks}: {current_date} to {chunk_end}"
        )

        try:
            account = AdAccount(account_id)

            # Define the insights query
            params = {
                "time_range": {
                    "since": current_date.strftime("%Y-%m-%d"),
                    "until": chunk_end.strftime("%Y-%m-%d"),
                },
                "level": level,
                "time_increment": 1,
                "breakdowns": table_config.get("breakdowns", []),
                "action_attribution_windows": table_config.get(
                    "action_attribution_windows", ["7d_click", "1d_view"]
                ),
            }

            # Get fields from config or use defaults
            fields = table_config.get(
                "fields",
                [
                    "account_id",
                    "account_name",
                    "campaign_id",
                    "campaign_name",
                    "adset_id",
                    "adset_name",
                    "ad_id",
                    "ad_name",
                    "date_start",
                    "date_stop",
                    "impressions",
                    "clicks",
                    "spend",
                    "reach",
                    "frequency",
                    "cpm",
                    "cpc",
                    "ctr",
                    "actions",
                    "action_values",
                    "conversions",
                    "conversion_values",
                    "cost_per_action_type",
                ],
            )

            # Filter fields based on level
            # CRITICAL: Be precise with field filtering to avoid excluding adset_id/adset_name
            if level == "account":
                # Account level: exclude campaign, adset, and ad fields
                fields = [
                    f
                    for f in fields
                    if not any(
                        f.startswith(prefix)
                        for prefix in ["campaign_", "adset_", "ad_"]
                    )
                ]
            elif level == "campaign":
                # Campaign level: exclude adset and ad fields, keep campaign fields
                fields = [
                    f
                    for f in fields
                    if not any(f.startswith(prefix) for prefix in ["adset_", "ad_"])
                ]
            elif level == "adset":
                # Adset level: exclude ONLY ad-specific fields (ad_id, ad_name)
                # KEEP adset_id, adset_name (they start with 'adset_' not 'ad_')
                # Exclude ad_name but keep ad_id if present
                ad_fields_to_exclude = ["ad_name"]  # Exclude ad_name but not ad_id
                fields = [f for f in fields if f not in ad_fields_to_exclude]

            # Create async job
            async_job = account.get_insights(
                fields=fields, params=params, is_async=True
            )

            # CRITICAL: Store the ENTIRE async_job object, not just the ID
            # The Facebook SDK AdReportRun object has proper type handling
            # Extracting just the ID and recreating AdReportRun causes truncation issues
            job_id = async_job["id"]
            logger.info(f"  Job created: {job_id}")

            all_jobs.append(
                {
                    "job_id": str(job_id),  # Keep as string for logging
                    "async_job": async_job,  # Store the actual AdReportRun object
                    "chunk_num": chunk_count,
                    "date_range": (current_date, chunk_end),
                    "date_start": current_date.strftime(
                        "%Y-%m-%d"
                    ),  # For error logging
                    "date_end": chunk_end.strftime("%Y-%m-%d"),  # For error logging
                    "status": "Job Running",
                    "percent_complete": 0,
                }
            )

        except Exception as e:
            logger.error(f"  Failed to create job: {str(e)[:100]}")

        current_date = chunk_end + timedelta(days=1)

        if test_mode and chunk_count >= 2:
            break

    # Step 2: Wait for all jobs to complete
    completed_jobs = wait_for_async_jobs(all_jobs)

    # Step 3: Retrieve results from completed jobs
    logger.info(f"\n STEP 3: Retrieving results from completed jobs...")

    for job_info in completed_jobs:
        if job_info["status"] == "Job Completed":
            logger.info(
                f"Retrieving results from job {job_info['chunk_num']}/{total_chunks}"
            )

            try:
                # Use the stored async_job object which has proper account context
                # DO NOT recreate AdReportRun from string ID - causes truncation
                async_job = job_info["async_job"]

                # Retry cursor pagination on transient Facebook 500 errors
                max_retries = 3
                job_records = 0
                for attempt in range(1, max_retries + 1):
                    try:
                        insights_cursor = async_job.get_insights()
                        records_this_attempt = 0
                        for insight in insights_cursor:
                            insight_data = dict(insight)
                            insight_data["account_id"] = account_id
                            insight_data["extracted_at"] = (
                                datetime.now()
                            )  # Keep as datetime for Parquet TIMESTAMP

                            # Convert timestamp string fields to datetime objects for proper Parquet TIMESTAMP type
                            timestamp_fields = ["date_start", "date_stop"]
                            for ts_field in timestamp_fields:
                                if (
                                    ts_field in insight_data
                                    and insight_data[ts_field]
                                    and isinstance(insight_data[ts_field], str)
                                ):
                                    try:
                                        from dateutil import parser

                                        insight_data[ts_field] = parser.parse(
                                            insight_data[ts_field]
                                        )
                                    except Exception as e:
                                        logger.debug(
                                            f"Could not parse {ts_field} '{insight_data[ts_field]}': {e}"
                                        )

                            # Process complex fields
                            for field in [
                                "actions",
                                "action_values",
                                "conversions",
                                "conversion_values",
                                "cost_per_action_type",
                                "cost_per_unique_action_type",
                                "video_play_actions",
                                "video_p25_watched_actions",
                                "video_p50_watched_actions",
                                "video_p75_watched_actions",
                                "video_p100_watched_actions",
                            ]:
                                if field in insight_data and insight_data[field]:
                                    # Flatten important metrics
                                    if isinstance(insight_data[field], list):
                                        for item in insight_data[field]:
                                            if "action_type" in item:
                                                # Fully sanitize to a stable BigQuery
                                                # column name. Previously only dots
                                                # were replaced, so an action_type with
                                                # spaces/dashes (e.g. "...MS - Ad
                                                # impression") kept those chars, got
                                                # re-sanitized differently by BQ on
                                                # load, and was re-added as a NEW _1
                                                # column every run (accreting junk).
                                                metric_name = sanitize_bq_column_name(
                                                    item["action_type"]
                                                )
                                                value = item.get("value", 0)
                                                # Convert to float for proper Parquet FLOAT type (Facebook returns strings)
                                                # This ensures actions/cost_per fields are FLOAT in BigQuery, not STRING
                                                try:
                                                    value = (
                                                        float(value) if value else 0.0
                                                    )
                                                except (ValueError, TypeError):
                                                    value = 0.0
                                                insight_data[
                                                    f"{field}_{metric_name}"
                                                ] = value
                                        # Store original as JSON
                                        insight_data[f"{field}_json"] = json.dumps(
                                            insight_data[field]
                                        )
                                    else:
                                        insight_data[f"{field}_json"] = json.dumps(
                                            insight_data[field]
                                        )

                            yield insight_data
                            job_records += 1
                            records_this_attempt += 1
                        # Cursor fully consumed, break retry loop
                        break
                    except GeneratorExit:
                        raise
                    except Exception as cursor_err:
                        error_str = str(cursor_err)
                        is_transient = any(
                            s in error_str
                            for s in [
                                "Status:  500",
                                "Status: 500",
                                "ConnectionError",
                                "Connection aborted",
                            ]
                        )
                        if is_transient and attempt < max_retries:
                            wait_secs = 30 * attempt
                            logger.warning(
                                f"  Cursor pagination error on attempt {attempt}/{max_retries} "
                                f"(got {records_this_attempt} records before failure). "
                                f"Retrying in {wait_secs}s: {error_str[:120]}"
                            )
                            time.sleep(wait_secs)
                            # Reset — re-fetch cursor from the beginning on retry
                            # Records already yielded will be deduplicated by hash merge in BigQuery
                            job_records = 0
                        else:
                            raise

                logger.info(
                    f"  Retrieved {job_records} records from job {job_info['chunk_num']}"
                )

            except GeneratorExit:
                raise
            except Exception as e:
                logger.error(
                    f"  Failed to retrieve results from job {job_info['chunk_num']}: {str(e)[:200]}"
                )
        else:
            logger.warning(
                f"  Job {job_info['chunk_num']} failed: {job_info.get('error', 'Unknown error')}"
            )

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info(f"ASYNC {level.upper()} INSIGHTS EXTRACTION COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Account: {account_id}")

    successful_jobs = [j for j in completed_jobs if j["status"] == "Job Completed"]
    logger.info(f"Successful jobs: {len(successful_jobs)}/{len(all_jobs)}")
    logger.info("=" * 60)

    # CRITICAL: Raise exception if all jobs failed
    # This prevents silent failures where pipeline shows "success" with 0 records
    if len(successful_jobs) == 0 and len(all_jobs) > 0:
        # Collect sample error messages from failed jobs
        error_samples = []
        for job in completed_jobs[:3]:  # Show first 3 errors
            if "error" in job:
                error_samples.append(
                    f"Job {job.get('chunk_num')}: {job['error'][:150]}"
                )

        error_msg = f"All {len(all_jobs)} async jobs failed for {level}_insights. Sample errors: {'; '.join(error_samples)}"
        logger.error(f"\n{error_msg}")
        raise Exception(error_msg)


def extract_leads(accounts: List[str]) -> Iterator[Dict]:
    """Extract leads from lead generation forms"""
    # Save original API configuration to restore later
    original_api = FacebookAdsApi.get_default_api()
    original_app_id = getattr(original_api, "app_id", None) if original_api else None
    original_app_secret = (
        getattr(original_api, "app_secret", None) if original_api else None
    )
    original_access_token = (
        getattr(original_api, "access_token", None) if original_api else None
    )

    try:
        for account_id in accounts:
            if not account_id.startswith("act_"):
                account_id = f"act_{account_id}"

            try:
                logger.info(f"Extracting leads for {account_id}")

                # Get pages with lead forms
                user = User(fbid="me")
                pages = user.get_accounts(fields=["id", "name", "access_token"])

                for page in pages:
                    page_id = page["id"]
                    page_name = page.get("name", "Unknown")
                    page_token = page.get("access_token")

                    if not page_token:
                        continue

                    try:
                        # Create temporary API instance with page-specific access token
                        # Use FacebookSession to avoid modifying global singleton
                        from facebook_business.session import FacebookSession

                        temp_session = FacebookSession(
                            app_id=original_app_id,
                            app_secret=original_app_secret,
                            access_token=page_token,
                        )
                        temp_api = FacebookAdsApi(temp_session, api_version="v25.0")
                        page_obj = Page(page_id, api=temp_api)

                        forms = page_obj.get_leadgen_forms(
                            fields=[
                                "id",
                                "name",
                                "status",
                                "created_time",
                                "leads_count",
                            ],
                            params={"limit": 100},
                        )

                        for form in forms:
                            form_data = dict(form)
                            form_id = form_data["id"]
                            leads_count = form_data.get("leads_count", 0)

                            if leads_count > 0:
                                leadgen_form = LeadgenForm(form_id)
                                leads = leadgen_form.get_leads(
                                    fields=["id", "created_time", "field_data"],
                                    params={"limit": 1000},
                                )

                                for lead in leads:
                                    lead_dict = dict(lead)
                                    lead_data = {
                                        "lead_id": lead_dict.get("id"),
                                        "form_id": form_id,
                                        "form_name": form_data.get("name"),
                                        "page_id": page_id,
                                        "page_name": page_name,
                                        "created_time": lead_dict.get("created_time"),
                                        "account_id": account_id,
                                        "extracted_at": datetime.now().isoformat(),
                                    }

                                    # Parse field data
                                    field_data = lead_dict.get("field_data", [])
                                    for field in field_data:
                                        field_name = (
                                            field.get("name", "")
                                            .replace(" ", "_")
                                            .lower()
                                        )
                                        field_values = field.get("values", [])
                                        if field_values:
                                            field_value = (
                                                "; ".join(field_values)
                                                if len(field_values) > 1
                                                else field_values[0]
                                            )
                                            field_name = "".join(
                                                c if c.isalnum() or c == "_" else "_"
                                                for c in field_name
                                            )
                                            lead_data[f"field_{field_name}"] = (
                                                field_value
                                            )

                                    lead_data["field_data_json"] = json.dumps(
                                        field_data
                                    )
                                    yield lead_data

                                logger.info(
                                    f"  Extracted {leads_count} leads from form {form_data.get('name')}"
                                )

                    except Exception as e:
                        logger.debug(f"Page {page_name} error: {e}")

            except Exception as e:
                logger.error(f"Error extracting leads for {account_id}: {e}")

    finally:
        # Restore original API configuration
        if original_access_token:
            FacebookAdsApi.init(
                original_app_id,
                original_app_secret,
                original_access_token,
                api_version="v25.0",
            )
            logger.debug("Restored original API configuration after leads extraction")


def extract_ad_page_mapping(accounts: List[str]) -> Iterator[Dict]:
    """Extract mapping between ads and Facebook pages"""
    for account_id in accounts:
        if not account_id.startswith("act_"):
            account_id = f"act_{account_id}"

        try:
            logger.info(f"Extracting ad-page mappings for {account_id}")
            account = AdAccount(account_id)

            ads = account.get_ads(
                fields=["id", "name", "campaign_id", "adset_id", "creative"]
            )

            for ad in ads:
                ad_data = dict(ad)
                creative_id = None

                # Extract creative ID
                if "creative" in ad_data:
                    creative_obj = ad_data["creative"]
                    if isinstance(creative_obj, dict):
                        creative_id = creative_obj.get("id")
                    elif hasattr(creative_obj, "_data"):
                        creative_id = creative_obj._data.get("id")

                if creative_id:
                    try:
                        creative = AdCreative(creative_id)
                        creative_fields = creative.api_get(
                            fields=[
                                "object_story_spec",
                                "object_story_id",
                                "effective_object_story_id",
                            ]
                        )

                        page_id = None

                        # Try to extract page_id
                        if "object_story_spec" in creative_fields:
                            story_spec = serialize_facebook_object(
                                creative_fields["object_story_spec"]
                            )
                            if isinstance(story_spec, dict):
                                page_id = story_spec.get("page_id")

                        if not page_id and creative_fields.get("object_story_id"):
                            story_id = str(creative_fields["object_story_id"])
                            if "_" in story_id:
                                page_id = story_id.split("_")[0]

                        if page_id:
                            mapping = {
                                "ad_id": ad_data["id"],
                                "ad_name": ad_data.get("name"),
                                "campaign_id": ad_data.get("campaign_id"),
                                "adset_id": ad_data.get("adset_id"),
                                "creative_id": creative_id,
                                "page_id": page_id,
                                "account_id": account_id,
                                "extracted_at": datetime.now().isoformat(),
                            }
                            yield mapping

                    except Exception as e:
                        logger.debug(f"Error processing creative {creative_id}: {e}")

        except Exception as e:
            logger.error(f"Error extracting ad-page mappings: {e}")


def extract_pages(accounts: List[str]) -> Iterator[Dict]:
    """
    Extract Facebook pages using multiple approaches.
    Tries me/accounts endpoint first, falls back to ad account campaigns if needed.
    """
    pages_dict = {}  # Use dict to dedupe by page_id

    try:
        from facebook_business.adobjects.user import User
        from facebook_business.adobjects.adaccount import AdAccount
        from facebook_business.adobjects.page import Page

        # Approach 1: Try User.get_accounts() (works with User Access Tokens)
        try:
            user = User(fbid="me")
            pages = user.get_accounts(
                fields=[
                    "id",
                    "name",
                    "username",
                    "link",
                    "about",
                    "category",
                    "category_list",
                    "fan_count",
                    "followers_count",
                    "is_verified",
                    "is_published",
                    "access_token",
                    "can_post",
                    "can_checkin",
                    "checkins",
                    "is_owned",
                    "is_permanently_closed",
                    "talking_about_count",
                    "were_here_count",
                    "cover",
                    "picture",
                    "overall_star_rating",
                    "rating_count",
                    "verification_status",
                    "has_transitioned_to_new_page_experience",
                ]
            )

            for page in pages:
                page_data = dict(page)
                pages_dict[page_data["id"]] = page_data

            logger.info(f"Retrieved {len(pages_dict)} pages via User.get_accounts()")

        except Exception as user_error:
            logger.debug(f"User.get_accounts() error: {str(user_error)[:200]}")

        # Approach 2: Always supplement with campaign-discovered pages
        logger.info(f"Supplementing with pages from ad account campaigns...")
        campaign_pages_found = 0
        for account_id in accounts:
            if not account_id.startswith("act_"):
                account_id = f"act_{account_id}"

            try:
                account = AdAccount(account_id)

                # Get campaigns to find promoted pages
                campaigns = list(
                    account.get_campaigns(fields=["id", "name", "promoted_object"])
                )
                logger.debug(f"Account {account_id}: Found {len(campaigns)} campaigns")

                for campaign in campaigns:
                    campaign_data = dict(campaign)
                    promoted_obj = campaign_data.get("promoted_object", {})

                    if isinstance(promoted_obj, dict) and "page_id" in promoted_obj:
                        page_id = promoted_obj["page_id"]

                        # Get full page details if not already retrieved
                        if page_id not in pages_dict:
                            try:
                                page = Page(page_id)
                                page_info = page.api_get(
                                    fields=[
                                        "id",
                                        "name",
                                        "username",
                                        "link",
                                        "about",
                                        "category",
                                        "category_list",
                                        "fan_count",
                                        "followers_count",
                                        "is_verified",
                                        "is_published",
                                        "can_post",
                                        "can_checkin",
                                        "checkins",
                                        "is_owned",
                                        "is_permanently_closed",
                                        "talking_about_count",
                                        "were_here_count",
                                        "cover",
                                        "picture",
                                        "overall_star_rating",
                                        "rating_count",
                                        "verification_status",
                                        "has_transitioned_to_new_page_experience",
                                    ]
                                )
                                pages_dict[page_id] = dict(page_info)
                                campaign_pages_found += 1
                                logger.debug(
                                    f"Added campaign page: {page_info.get('name', page_id)}"
                                )
                            except Exception as page_error:
                                logger.debug(
                                    f"Could not get details for page {page_id}: {page_error}"
                                )
                                # Create minimal page record
                                pages_dict[page_id] = {"id": page_id, "name": None}
                                campaign_pages_found += 1

            except Exception as account_error:
                logger.debug(
                    f"Could not get campaign pages from account {account_id}: {str(account_error)[:100]}"
                )
                continue

        if campaign_pages_found > 0:
            logger.info(
                f"Added {campaign_pages_found} pages from campaign discovery (total: {len(pages_dict)}, {len(pages_dict) - campaign_pages_found} from User.get_accounts)"
            )
        else:
            logger.debug(
                f"No additional pages found via campaign discovery (all {len(pages_dict)} from User.get_accounts)"
            )

        # Process and yield all pages
        for page_data in pages_dict.values():
            # Serialize complex fields - Facebook SDK objects need special handling
            for key, value in list(page_data.items()):
                if value is None:
                    continue
                # Check if it's a Facebook SDK object (has export_all_data method)
                if hasattr(value, "export_all_data"):
                    page_data[key] = json.dumps(value.export_all_data())
                elif isinstance(value, (dict, list)):
                    page_data[key] = serialize_facebook_object(value)
                elif not isinstance(value, (str, int, float, bool, type(None))):
                    # Unknown object type - convert to string
                    page_data[key] = serialize_facebook_object(value)

            # Add account context (use canonical FACEBOOK_ACCOUNT_ID from env, not iteration order)
            if "account_id" not in page_data:
                page_data["account_id"] = _canonical_account_id(accounts)

            yield page_data

    except Exception as e:
        logger.error(f"Error extracting pages: {e}")
        logger.debug(f"Pages extraction error details: {str(e)}", exc_info=True)


def extract_posts(
    accounts: List[str], pages: List[Dict] = None, date_range: tuple = None
) -> Iterator[Dict]:
    """
    Extract Facebook posts from pages using the Page object's feed.
    Requires pages to be extracted first.
    Uses 90-day batching for date ranges exceeding Facebook's 93-day API limit.
    Populates post spool for dependent tables (post_insights, post_attachments).

    Args:
        accounts: List of Facebook account IDs
        pages: Optional list of page dictionaries (will extract if not provided)
        date_range: Tuple of (start_date, end_date) as date objects
    """
    try:
        from facebook_business.adobjects.page import Page
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import time

        # Initialize tracking for failed pages
        if not hasattr(extract_posts, "_failed_pages"):
            extract_posts._failed_pages = []
        if not hasattr(extract_posts, "_transient_error_count"):
            extract_posts._transient_error_count = 0

        # If pages not provided, extract them first
        if not pages:
            logger.info("Extracting pages first to get posts...")
            pages = list(extract_pages(accounts))

        if not pages:
            logger.warning("No pages found, cannot extract posts")
            return

        # Initialize post spool for this extraction
        spool = get_current_spool()
        if not spool:
            spool = create_spool()
        elif spool.size() > 0:
            # A dependency fetch already populated the spool this run; serve
            # from it instead of re-fetching every page. The spool keeps
            # page_access_token, which must never reach persisted output.
            logger.info(
                f"Serving {spool.size()} posts from spool (already extracted this run)"
            )
            for spooled_post in spool.get_all_full_posts():
                post_output = spooled_post.copy()
                post_output.pop("page_access_token", None)
                post_output.pop("access_token", None)
                yield post_output
            return

        logger.info(f"Extracting posts from {len(pages)} pages")

        # Calculate 90-day batches if date range provided and exceeds 90 days
        date_batches = []
        if date_range:
            start_date, end_date = date_range
            days_diff = (end_date - start_date).days

            if days_diff > 90:
                # Split into 90-day batches (90 days inclusive means 91 day spans)
                # Fix: ensure no overlap at boundaries by starting next batch at current_end
                logger.info(
                    f"Date range spans {days_diff} days - splitting into 90-day batches"
                )
                current_start = start_date
                while current_start < end_date:
                    # 90 days from start: if start=Jan1, 90 days is Jan 1-Mar 31 (89 days apart)
                    # Add 89 days (90 day span) to get non-overlapping boundaries
                    current_end = min(current_start + timedelta(days=89), end_date)
                    date_batches.append((current_start, current_end))
                    # Start next batch the day after current_end to avoid overlap
                    current_start = current_end + timedelta(days=1)
                logger.info(f"Created {len(date_batches)} batches for extraction")
            else:
                # Single batch
                date_batches = [(start_date, end_date)]
        else:
            # No date filtering
            date_batches = [None]

        # Process pages sequentially (Facebook best practice to avoid rate limits)
        total_posts_extracted = 0
        pages_with_deprecation_issues = 0

        for page_idx, page in enumerate(pages, 1):
            page_id = page.get("id")
            page_access_token = page.get("access_token")

            if not page_id or not page_access_token:
                logger.debug(f"  Skipping page {page_id}: No access token")
                continue

            try:
                # Use Page SDK object with the page's access token
                fb_page = Page(page_id)
                page_total_posts = 0

                # Track deprecated fields for THIS PAGE (persists across batches for same page)
                page_deprecated_fields = set()

                # Process each date batch
                for batch_idx, batch in enumerate(date_batches, 1):
                    # Build params for this batch
                    params = {"access_token": page_access_token, "limit": 100}

                    if batch:
                        # Add date filtering - Facebook expects Unix timestamps
                        batch_start, batch_end = batch
                        params["since"] = int(
                            datetime.combine(
                                batch_start, datetime.min.time()
                            ).timestamp()
                        )
                        params["until"] = int(
                            datetime.combine(batch_end, datetime.max.time()).timestamp()
                        )

                        if len(date_batches) > 1:
                            logger.info(
                                f"  Page {page_idx}/{len(pages)}: Processing batch {batch_idx}/{len(date_batches)} ({batch_start} to {batch_end})"
                            )

                    # Get feed posts using the page access token
                    # NOTE: Using get_feed() retrieves all posts (published and shared). Config says published_posts
                    # but get_feed() is correct for our use case (all posts page interacts with).
                    # comments.summary(true) and shares emit a benign SDK UserWarning but DO return data
                    # (comment count parsed below); do NOT remove them. picture/link/object_id were removed
                    # as truly invalid on the feed endpoint.
                    all_fields = [
                        "id",
                        "message",
                        "created_time",
                        "updated_time",
                        "permalink_url",
                        "status_type",
                        "full_picture",
                        "is_hidden",
                        "is_published",
                        "from{id,name}",
                        "icon",
                        "admin_creator",
                        "privacy",
                        "timeline_visibility",
                        "is_expired",
                        "is_popular",
                        "is_spherical",
                        "story",
                        "parent_id",
                        "call_to_action",
                        "scheduled_publish_time",
                        "shares",
                        "comments.summary(true)",
                    ]

                    # Deprecated fields in Graph API v25.0+ (may not be available)
                    # Try progressively removing fields if we hit deprecation errors
                    posts_cursor = None
                    fields_to_try = [
                        f for f in all_fields if f not in page_deprecated_fields
                    ]
                    page_had_deprecation = False

                    # List of potentially deprecated fields to remove one at a time (if hit deprecation errors)
                    potentially_deprecated = [
                        "full_picture",
                        "icon",
                        "story",
                        "is_spherical",
                    ]

                    for attempt in range(len(potentially_deprecated) + 1):
                        try:
                            wait_if_needed("facebook_posts")
                            posts_cursor = fb_page.get_feed(
                                fields=fields_to_try, params=params
                            )
                            reset_rate_limit("facebook_posts")
                            break  # Success!
                        except Exception as e:
                            error_str = str(e)
                            # Check if error is about deprecated fields
                            if (
                                "deprecate" in error_str.lower()
                                or "deprecated" in error_str.lower()
                            ):
                                if attempt < len(potentially_deprecated):
                                    # Remove next potentially deprecated field and retry
                                    field_to_remove = potentially_deprecated[attempt]
                                    if field_to_remove in fields_to_try:
                                        page_deprecated_fields.add(field_to_remove)
                                        page_had_deprecation = True
                                        fields_to_try = [
                                            f
                                            for f in fields_to_try
                                            if f != field_to_remove
                                        ]
                                    else:
                                        # Field already removed, try next one
                                        continue
                                else:
                                    # Tried removing all deprecated fields, still failing
                                    logger.error(
                                        f"  Still failing after removing all deprecated fields for page {page_id}"
                                    )
                                    raise
                            else:
                                # Not a deprecation error, re-raise
                                raise

                    if page_had_deprecation:
                        pages_with_deprecation_issues += 1

                    if posts_cursor is None:
                        logger.error(f"  Failed to get posts for page {page_id}")
                        continue

                    # Iterate through results (handles pagination automatically)
                    batch_post_count = 0
                    for post in posts_cursor:
                        # Convert SDK object to dict
                        post_dict = dict(post)

                        # Handle 'from' field specially - convert object to JSON string
                        if "from" in post_dict and isinstance(post_dict["from"], dict):
                            post_dict["from"] = json.dumps(post_dict["from"])

                        # Handle comments field - extract count from summary
                        if "comments" in post_dict and isinstance(
                            post_dict["comments"], dict
                        ):
                            comments_count = (
                                post_dict["comments"]
                                .get("summary", {})
                                .get("total_count", 0)
                            )
                            post_dict["comments_count"] = comments_count
                            del post_dict["comments"]  # Remove the complex object

                        # Handle shares field - extract count
                        if "shares" in post_dict and isinstance(
                            post_dict["shares"], dict
                        ):
                            shares_count = post_dict["shares"].get("count", 0)
                            post_dict["shares_count"] = shares_count
                            del post_dict["shares"]  # Remove the complex object

                        # Serialize complex fields
                        for key, value in list(post_dict.items()):
                            if isinstance(value, (dict, list)):
                                post_dict[key] = serialize_facebook_object(value)
                            # Handle SDK objects
                            elif not isinstance(
                                value, (str, int, float, bool, type(None))
                            ):
                                post_dict[key] = serialize_facebook_object(value)

                        # Ensure deprecated fields exist (fill with None if not returned by API)
                        # This maintains schema consistency when fields are deprecated
                        for deprecated_field in page_deprecated_fields:
                            if deprecated_field not in post_dict:
                                post_dict[deprecated_field] = None

                        # Add context fields (use canonical account_id from env to avoid page-ID mislabeling)
                        post_dict["account_id"] = _canonical_account_id(accounts)
                        post_dict["page_id"] = page_id
                        post_dict["page_name"] = page.get(
                            "name", ""
                        )  # Add page name for post insights
                        post_dict["page_access_token"] = (
                            page_access_token  # Needed for post insights extraction
                        )

                        # Populate post spool for dependent tables (post_insights, post_attachments)
                        spool.add_post(post_dict)

                        # Remove sensitive tokens from persisted output (kept only in spool)
                        post_dict_output = post_dict.copy()
                        post_dict_output.pop("page_access_token", None)
                        post_dict_output.pop("access_token", None)

                        yield post_dict_output
                        batch_post_count += 1
                        page_total_posts += 1
                        total_posts_extracted += 1

                    # Log batch completion
                    if len(date_batches) > 1 and batch_post_count > 0:
                        logger.info(
                            f"    Batch {batch_idx} complete: {batch_post_count} posts (Total: {total_posts_extracted})"
                        )

                # Log page summary
                if page_total_posts > 0:
                    logger.info(
                        f"  Page {page_idx}/{len(pages)} complete: {page_total_posts} posts"
                    )
                else:
                    logger.debug(f"  Page {page_idx}/{len(pages)}: No posts found")

            except Exception as e:
                # Check if this is a transient Facebook API error (HTTP 500, error code 1)
                error_str = str(e)
                is_transient_error = (
                    "500" in error_str
                    and "error" in error_str.lower()
                    and (
                        "code" in error_str.lower()
                        or "unknown error" in error_str.lower()
                    )
                )

                if is_transient_error:
                    extract_posts._transient_error_count += 1

                    # Track this page for reprocessing
                    extract_posts._failed_pages.append(
                        {
                            "page_id": page_id,
                            "page_name": page.get("name", "Unknown"),
                            "account_id": _canonical_account_id(accounts),
                            "error_type": "transient_api_error",
                            "error": "Facebook API HTTP 500 error",
                        }
                    )

                    logger.warning(
                        f"  Transient API error #{extract_posts._transient_error_count} for page {page_id} ({page.get('name', 'Unknown')})"
                    )
                    logger.warning(
                        f"  Skipping page due to temporary Facebook API issue"
                    )

                    # Fail if too many transient errors
                    if extract_posts._transient_error_count > 25:
                        logger.error(
                            f"CRITICAL: More than 25 transient API errors encountered"
                        )
                        logger.error(f"Failing extraction to prevent extended failures")
                        raise RuntimeError(
                            f"Too many transient Facebook API errors (>{25})"
                        )
                else:
                    # Check if this is a rate limit error
                    if any(
                        term in error_str
                        for term in [
                            "rate limit",
                            "too many requests",
                            "429",
                            "api call limit",
                            "throttled",
                        ]
                    ):
                        backoff_time = handle_rate_limit_error("facebook_posts", e)
                        logger.warning(
                            f"  Rate limit hit for page {page_id}, backing off {backoff_time:.1f}s before retry"
                        )
                        extract_posts._failed_pages.append(
                            {
                                "page_id": page_id,
                                "page_name": page.get("name", "Unknown"),
                                "account_id": _canonical_account_id(accounts),
                                "error_type": "rate_limit_error",
                                "error": "Rate limit exceeded",
                            }
                        )
                    else:
                        # Non-transient error - track and skip page
                        extract_posts._failed_pages.append(
                            {
                                "page_id": page_id,
                                "page_name": page.get("name", "Unknown"),
                                "account_id": _canonical_account_id(accounts),
                                "error_type": "extraction_error",
                                "error": str(e)[:100],  # Truncate long errors
                            }
                        )

                        logger.warning(
                            f"  Error extracting posts for page {page_id}: {e}"
                        )
                        logger.debug(f"  Full error details: {str(e)}", exc_info=True)

    except Exception as e:
        logger.error(f"Error extracting posts: {e}")
        logger.debug(f"Posts extraction error details: {str(e)}", exc_info=True)
    finally:
        # Display summary of failed pages if any
        if hasattr(extract_posts, "_failed_pages") and extract_posts._failed_pages:
            logger.warning("")
            logger.warning("=" * 80)
            logger.warning(
                f"POSTS EXTRACTION SUMMARY: {len(extract_posts._failed_pages)} page(s) failed"
            )
            logger.warning("=" * 80)

            # Group by error type
            transient_errors = [
                p
                for p in extract_posts._failed_pages
                if p["error_type"] == "transient_api_error"
            ]
            other_errors = [
                p
                for p in extract_posts._failed_pages
                if p["error_type"] != "transient_api_error"
            ]

            if transient_errors:
                logger.warning(
                    f"\nTransient API Errors ({len(transient_errors)} pages):"
                )
                logger.warning(
                    "These pages failed due to temporary Facebook API issues."
                )
                for page in transient_errors:
                    logger.warning(
                        f"- Page: {page['page_name']} (ID: {page['page_id']})"
                    )

            if other_errors:
                logger.warning(
                    f"\nOther Extraction Errors ({len(other_errors)} pages):"
                )
                for page in other_errors:
                    logger.warning(
                        f"- Page: {page['page_name']} (ID: {page['page_id']})"
                    )
                    logger.warning(f"  Error: {page['error']}")

            # Provide reprocessing command with date range
            logger.warning("\n" + "=" * 80)
            logger.warning("AUTOMATIC RETRY FOR FAILED PAGES")
            logger.warning("=" * 80)

            # Get unique page IDs and date range. The window comes from this
            # function's date_range tuple (start, end) -- not a config dict, which is
            # not in scope here (the prior `config.get(...)` was an undefined-name bug,
            # flake8 F821).
            failed_page_ids = [p["page_id"] for p in extract_posts._failed_pages]
            page_ids_str = ",".join(failed_page_ids)
            if date_range and len(date_range) == 2:
                start_date, end_date = date_range[0], date_range[1]
            else:
                start_date, end_date = "UNKNOWN", "UNKNOWN"

            # Only retry transient errors (not permanent failures)
            transient_failed = [
                p
                for p in extract_posts._failed_pages
                if p.get("error_type") == "transient_api_error"
            ]
            if transient_failed:
                transient_ids = [p["page_id"] for p in transient_failed]
                transient_ids_str = ",".join(transient_ids)

                logger.warning(
                    f"\nRetrying {len(transient_ids)} page(s) with transient errors"
                )
                logger.warning(f"Data Range: {start_date} to {end_date}")

                # Automatic retry: call extract_posts recursively with only failed pages
                logger.warning(
                    f"Executing: extract_posts(accounts, date_range, config, pages, sites=[{transient_ids_str}])"
                )

                # Re-extract posts for failed pages only
                retry_pages = [p for p in pages if p.get("id") in transient_ids]
                if retry_pages:
                    logger.warning(
                        f"\nStarting automatic retry for {len(retry_pages)} failed page(s)..."
                    )
                    time.sleep(5)  # Brief delay to allow API recovery

                    # Reset transient error count for retry
                    extract_posts._transient_error_count = 0
                    retry_failed_count_before = len(
                        [
                            p
                            for p in extract_posts._failed_pages
                            if p.get("error_type") == "transient_api_error"
                        ]
                    )

                    # Extract posts for failed pages only (correct signature: accounts, pages, date_range)
                    for post_batch in extract_posts(
                        accounts, pages=retry_pages, date_range=date_range
                    ):
                        yield post_batch

                    retry_failed_count_after = len(
                        [
                            p
                            for p in extract_posts._failed_pages
                            if p.get("error_type") == "transient_api_error"
                        ]
                    )

                    if retry_failed_count_after < retry_failed_count_before:
                        logger.warning(
                            f"Automatic retry recovered {retry_failed_count_before - retry_failed_count_after} page(s)"
                        )
                    else:
                        logger.warning(
                            f"Automatic retry did not recover additional pages"
                        )

            # Log remaining failures and provide manual command
            remaining_failed = [
                p
                for p in extract_posts._failed_pages
                if p.get("error_type") == "transient_api_error"
            ]
            if remaining_failed:
                logger.warning("\n" + "=" * 80)
                logger.warning("MANUAL REPROCESSING COMMAND (for persistent failures):")
                logger.warning("=" * 80)

                remaining_ids = [p["page_id"] for p in remaining_failed]
                remaining_ids_str = ",".join(remaining_ids)

                if len(remaining_ids) <= 5:
                    logger.warning(f"\nData Range: {start_date} to {end_date}")
                    logger.warning(
                        f"python orchestrate.py --source facebook --env prod --tables posts --start-date {start_date} --end-date {end_date} --sites {remaining_ids_str}"
                    )
                else:
                    logger.warning(
                        f"\nToo many failed pages ({len(remaining_ids)}) - retry with full range:"
                    )
                    logger.warning(
                        f"python orchestrate.py --source facebook --env prod --tables posts --start-date {start_date} --end-date {end_date}"
                    )

            logger.warning("\n" + "=" * 80)
            logger.warning("")


def ensure_dataset_exists(client: bigquery.Client, dataset_id: str):
    """Create dataset if it doesn't exist"""
    try:
        client.get_dataset(dataset_id)
    except:
        dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
        dataset.location = "US"
        dataset = client.create_dataset(dataset)
        logger.info(f"Created dataset: {dataset_id}")


def extract_page_insights(
    accounts: List[str], date_range: tuple, config: Dict, pages: List[Dict] = None
) -> Iterator[Dict]:
    """
    Extract page-level insights from Facebook using the page insights API.

    Args:
        accounts: List of Facebook account IDs
        date_range: Tuple of (start_date, end_date)
        config: Table configuration from YAML

    Yields:
        Dict records with page insights data
    """
    from facebook_business.adobjects.page import Page
    from datetime import datetime, timedelta

    # Save original API configuration to restore later
    original_api = FacebookAdsApi.get_default_api()
    original_app_id = getattr(original_api, "app_id", None) if original_api else None
    original_app_secret = (
        getattr(original_api, "app_secret", None) if original_api else None
    )
    original_access_token = (
        getattr(original_api, "access_token", None) if original_api else None
    )

    try:
        # Use pre-discovered pages if provided, otherwise fetch them now
        if not pages:
            pages = list(extract_pages(accounts))

        if not pages:
            logger.warning("No pages found to extract insights from")
            return

        logger.info(f"Extracting insights for {len(pages)} pages")

        # Get metric list from config
        metrics = config.get("params", {}).get("metric", [])
        if not metrics:
            logger.warning("No metrics defined in config for page_insights")
            return

        logger.info(f"  Loaded {len(metrics)} metrics from config")

        # Calculate 90-day batches for the date range (Facebook limit is 93 days)
        start_date, end_date = date_range
        total_days = (end_date - start_date).days

        if total_days > 90:
            logger.info(f"Date range spans {total_days} days - using 90-day batching")
            batches = []
            current = start_date
            while current < end_date:
                batch_end = min(current + timedelta(days=90), end_date)
                batches.append((current, batch_end))
                current = batch_end  # until is exclusive, so batch_end was not included
            logger.info(f"Created {len(batches)} batches of ~90 days each")
        else:
            batches = [(start_date, end_date)]

        # Process each page
        page_count = 0
        for page_data in pages:
            page_count += 1
            page_id = page_data["id"]
            page_name = page_data.get("name", "Unknown")
            account_id = page_data.get("account_id")
            page_access_token = page_data.get("access_token")
            followers_count = page_data.get(
                "followers_count", page_data.get("fan_count", 0)
            )  # Use followers_count or fall back to fan_count

            if not page_access_token:
                logger.warning(
                    f"  Skipping page {page_name} ({page_id}) - no access token available"
                )
                continue

            logger.info(f"  [{page_count}/{len(pages)}] Processing page: {page_name}")

            # Process each batch
            for batch_start, batch_end in batches:
                try:
                    # Rate limit: wait before making API call to prevent hammering Facebook
                    wait_if_needed("facebook_page_insights")

                    # Create temporary API instance with page-specific access token
                    # Use FacebookSession to avoid modifying global singleton
                    from facebook_business.session import FacebookSession

                    temp_session = FacebookSession(
                        app_id=original_app_id,
                        app_secret=original_app_secret,
                        access_token=page_access_token,
                    )
                    temp_api = FacebookAdsApi(temp_session, api_version="v25.0")

                    # Create Page object with temporary API
                    page = Page(fbid=page_id, api=temp_api)

                    # Call insights API with batched date range (with retry for transient errors)
                    # Note: Page insights API expects metrics in params, not fields
                    # DEFENSIVE: If metrics fail (deprecated/invalid), continue with empty records
                    def _fetch_page_insights():
                        return page.get_insights(
                            params={
                                "metric": metrics,
                                "since": batch_start.strftime("%Y-%m-%d"),
                                "until": batch_end.strftime("%Y-%m-%d"),
                                "period": "day",
                            }
                        )

                    try:
                        from shared.api_retry import with_exponential_backoff

                        insights_fn = with_exponential_backoff(
                            resource_name=f"page_insights:{page_name}", max_retries=3
                        )(_fetch_page_insights)
                        insights = insights_fn()
                        reset_rate_limit("facebook_page_insights")
                    except Exception as e:
                        error_msg = str(e)
                        if (
                            "valid insights metric" in error_msg.lower()
                            or "deprecated" in error_msg.lower()
                        ):
                            logger.warning(
                                f"    Facebook deprecated metrics for {page_name} - some/all metrics unavailable: {error_msg[:200]}"
                            )
                            # Create empty records with just metadata (page_id, date, followers)
                            # This allows pipeline to continue and preserves data lineage
                            for single_date in [
                                batch_start + timedelta(days=x)
                                for x in range((batch_end - batch_start).days + 1)
                            ]:
                                yield {
                                    "account_id": (
                                        str(account_id)
                                        if account_id is not None
                                        else ""
                                    ),
                                    "page_id": str(page_id),
                                    "page_name": page_name,
                                    "date": single_date.strftime("%Y-%m-%d"),
                                    "followers_count": (
                                        str(int(followers_count))
                                        if followers_count
                                        else "0"
                                    ),
                                }
                            continue  # Skip to next batch
                        else:
                            # Re-raise unexpected errors
                            raise

                    # Aggregate metrics by (page_id, date)
                    records_by_date: Dict[Tuple[str, str], Dict[str, Any]] = {}

                    def _normalize_metric_value(raw: Any) -> Any:
                        if raw in (None, ""):
                            return 0.0
                        if isinstance(raw, (int, float)):
                            val = float(raw)
                            return 0.0 if val != val else val  # NaN guard
                        if isinstance(raw, str):
                            try:
                                return float(raw)
                            except ValueError:
                                return raw
                        if isinstance(raw, dict):
                            for key in ("value", "total", "count"):
                                if key in raw:
                                    normalized = _normalize_metric_value(raw[key])
                                    if isinstance(normalized, (int, float, float)):
                                        return float(normalized)
                                    return normalized
                            if len(raw) == 1:
                                return _normalize_metric_value(next(iter(raw.values())))
                            return raw
                        if isinstance(raw, list):
                            if len(raw) == 1:
                                return _normalize_metric_value(raw[0])
                            return raw
                        return raw

                    def _format_metric_value(value: Any) -> Any:
                        if isinstance(value, (int, float)):
                            # NaN check — float('nan') != float('nan') is True
                            if isinstance(value, float) and (value != value):
                                return "0"
                            if isinstance(value, float) and value.is_integer():
                                return str(int(value))
                            return str(value)
                        return value

                    metrics_returned = set()
                    insight_count = 0
                    for insight in insights:
                        insight_data = dict(insight)

                        metric_name = insight_data.get("name")
                        if not metric_name:
                            continue

                        metrics_returned.add(metric_name)
                        insight_count += 1
                        values_list = insight_data.get("values", [])

                        sample_logged = False
                        for value_item in values_list:
                            end_time = value_item.get("end_time", "")
                            date_str = end_time.split("T")[0] if end_time else None
                            if not date_str:
                                continue

                            key = (page_id, date_str)
                            record = records_by_date.setdefault(
                                key,
                                {
                                    "account_id": (
                                        str(account_id)
                                        if account_id is not None
                                        else ""
                                    ),
                                    "page_id": str(page_id),
                                    "page_name": page_name,
                                    "date": date_str,
                                    "followers_count": (
                                        str(int(followers_count))
                                        if followers_count
                                        else "0"
                                    ),
                                },
                            )

                            raw_value = value_item.get("value", 0)
                            metric_value: Any = _normalize_metric_value(raw_value)

                            if (
                                metric_name == "page_actions_post_reactions_total"
                                and isinstance(raw_value, dict)
                            ):
                                aggregated_total = 0.0
                                for reaction_type, reaction_val in raw_value.items():
                                    normalized_reaction = _normalize_metric_value(
                                        reaction_val
                                    )
                                    column_name = f"page_actions_post_reactions_{reaction_type}_total"
                                    if isinstance(normalized_reaction, (int, float)):
                                        normalized_numeric = float(normalized_reaction)
                                        record[column_name] = _format_metric_value(
                                            normalized_numeric
                                        )
                                        aggregated_total += normalized_numeric
                                    else:
                                        record[column_name] = normalized_reaction
                                if aggregated_total:
                                    metric_value = aggregated_total

                            if metric_name.startswith("page_actions_post_reactions_"):
                                existing = record.get(metric_name)
                                if existing not in (
                                    None,
                                    0,
                                    0.0,
                                    "0",
                                    "0.0",
                                ) and metric_value in (0, 0.0, "0", "0.0"):
                                    continue

                            if (
                                metric_name.startswith("page_actions_post_reactions_")
                                and not sample_logged
                            ):
                                logger.debug(
                                    "Page insight metric %s raw=%s normalized=%s (page_id=%s date=%s)",
                                    metric_name,
                                    raw_value,
                                    metric_value,
                                    page_id,
                                    date_str,
                                )
                                sample_logged = True

                            record[metric_name] = _format_metric_value(metric_value)

                    for record in records_by_date.values():
                        yield record

                except Exception as e:
                    error_str = str(e).lower()
                    if any(
                        term in error_str
                        for term in [
                            "rate limit",
                            "too many requests",
                            "429",
                            "api call limit",
                            "throttled",
                        ]
                    ):
                        backoff_time = handle_rate_limit_error(
                            "facebook_page_insights", e
                        )
                        logger.warning(
                            f"  Rate limit hit for page {page_id}, backing off {backoff_time:.1f}s before retry"
                        )
                        continue
                    elif "500" in error_str or (
                        "is_transient" in error_str
                        or "code" in error_str
                        and "2" in error_str
                    ):
                        logger.warning(
                            f"  Transient Facebook API error for page {page_id} ({batch_start} to {batch_end}): {str(e)[:300]}"
                        )
                        logger.warning(
                            f"  Yielding empty records for this batch to preserve data lineage"
                        )
                        for single_date in [
                            batch_start + timedelta(days=x)
                            for x in range((batch_end - batch_start).days + 1)
                        ]:
                            yield {
                                "account_id": (
                                    str(account_id) if account_id is not None else ""
                                ),
                                "page_id": str(page_id),
                                "page_name": page_name,
                                "date": single_date.strftime("%Y-%m-%d"),
                                "followers_count": (
                                    str(int(followers_count))
                                    if followers_count
                                    else "0"
                                ),
                            }
                        continue
                    else:
                        logger.warning(
                            f"  Failed to extract insights for page {page_id} ({batch_start} to {batch_end}): {e}"
                        )
                        continue

    except Exception as e:
        logger.error(f"Error extracting page insights: {e}")
        import traceback

        logger.debug(traceback.format_exc())

    finally:
        # Restore original API configuration
        if original_access_token:
            FacebookAdsApi.init(
                original_app_id,
                original_app_secret,
                original_access_token,
                api_version="v25.0",
            )
            logger.debug(
                "Restored original API configuration after page_insights extraction"
            )


def extract_post_insights(
    accounts: List[str],
    date_range: tuple,
    config: Dict,
    pages: List[Dict] = None,
    spool=None,
) -> Iterator[Dict]:
    """
    Extract post-level insights from Facebook using parent post spool.
    Avoids duplicate post fetches by using cached posts from spool.

    Args:
        accounts: List of Facebook account IDs
        date_range: Tuple of (start_date, end_date)
        config: Table configuration from YAML
        pages: Optional list of page dictionaries
        spool: Optional PostSpool instance (if None, will fetch posts fresh)

    Yields:
        Dict records with post insights data
    """
    from facebook_business.adobjects.pagepost import PagePost

    # Save original API configuration to restore later
    original_api = FacebookAdsApi.get_default_api()
    original_app_id = getattr(original_api, "app_id", None) if original_api else None
    original_app_secret = (
        getattr(original_api, "app_secret", None) if original_api else None
    )
    original_access_token = (
        getattr(original_api, "access_token", None) if original_api else None
    )

    try:
        # Use spool if available, otherwise get from extraction (triggers if post_insights without posts table)
        if spool is None or spool.size() == 0:
            spool = get_current_spool() or spool

        # If spool is empty, we need to extract posts first (post_insights requested without posts table)
        if not spool or spool.size() == 0:
            logger.info(
                "Post spool empty - extracting posts for post_insights dependency"
            )
            # extract_posts populates the current global spool (creating one if needed);
            # never create_spool() here or a populated global gets clobbered
            _ = list(extract_posts(accounts, pages=pages, date_range=date_range))
            spool = get_current_spool() or spool

        if not spool or spool.size() == 0:
            logger.warning("No posts found to extract insights from")
            return

        posts_for_insights = list(spool.get_all_manifests())

        if not posts_for_insights:
            logger.warning("No posts found to extract insights from")
            return

        logger.info(f"Extracting insights for {len(posts_for_insights)} posts")

        # Get metric list from config
        metrics = config.get("params", {}).get("metric", [])
        if not metrics:
            logger.warning("No metrics defined in config for post_insights")
            return

        # Group posts by page_access_token for batched API calls
        posts_by_token = {}
        for post_data in posts_for_insights:
            token = post_data.get("page_access_token")
            if token:
                if token not in posts_by_token:
                    posts_by_token[token] = []
                posts_by_token[token].append(post_data)

        logger.info(
            f"Grouped {len(posts_for_insights)} posts by {len(posts_by_token)} page access tokens for batching"
        )

        # Process posts grouped by page_access_token for efficient batching
        posts_processed = 0
        records_extracted = 0
        batch_size = 50  # Facebook batch API limit

        for token_idx, (page_token, token_posts) in enumerate(
            posts_by_token.items(), 1
        ):
            logger.debug(
                f"Processing token group {token_idx}/{len(posts_by_token)} with {len(token_posts)} posts"
            )

            # Process posts for this token in batches
            for batch_start in range(0, len(token_posts), batch_size):
                batch_end = min(batch_start + batch_size, len(token_posts))
                batch_posts = token_posts[batch_start:batch_end]

                logger.debug(
                    f"  Processing batch {batch_start // batch_size + 1} ({len(batch_posts)} posts)"
                )

                # Process each post in the batch
                for post_data in batch_posts:
                    post_id = post_data["id"]
                    account_id = post_data.get("account_id")
                    page_id = post_data.get("page_id")
                    page_access_token = post_data.get("page_access_token")

                    if not page_access_token:
                        logger.debug(
                            f"  Skipping post {post_id} - no page access token available"
                        )
                        continue

                    posts_processed += 1
                    if posts_processed % 100 == 0:
                        logger.info(
                            f"  Progress: {posts_processed}/{len(posts_for_insights)} posts processed, {records_extracted} records extracted"
                        )

                    logger.debug(f"  Extracting insights for post: {post_id}")

                    try:
                        # Create temporary API instance with page-specific access token
                        # Use FacebookSession to avoid modifying global singleton
                        from facebook_business.session import FacebookSession

                        temp_session = FacebookSession(
                            app_id=original_app_id,
                            app_secret=original_app_secret,
                            access_token=page_access_token,
                        )
                        temp_api = FacebookAdsApi(temp_session, api_version="v25.0")

                        # Create PagePost object with temporary API
                        post = PagePost(fbid=post_id, api=temp_api)

                        # Build base record with all post metadata
                        record = {
                            "account_id": account_id,
                            "page_id": page_id,
                            "post_id": post_id,
                            "page_name": post_data.get("page_name", ""),
                            "message": post_data.get("message", ""),
                            "created_time": post_data.get("created_time", ""),
                            "date": post_data.get("created_time", "").split("T")[0],
                            "comments_count": (
                                int(post_data.get("comments_count", 0))
                                if post_data.get("comments_count")
                                else 0
                            ),
                            "shares_count": (
                                int(post_data.get("shares_count", 0))
                                if post_data.get("shares_count")
                                else 0
                            ),
                            "link": post_data.get("link", ""),
                            "object_id": post_data.get("object_id", ""),
                        }

                        # Fetch all insights metrics in a single call (with retry for transient errors)
                        def _fetch_post_insights():
                            logger.debug(
                                f"    Requesting insights metrics for post {post_id}: {metrics}"
                            )
                            return post.get_insights(params={"metric": metrics})

                        # Default all reaction fields to 0.0; populated below from
                        # post_reactions_by_type_total if present.
                        reaction_fields = (
                            "post_reactions_like_total",
                            "post_reactions_love_total",
                            "post_reactions_wow_total",
                            "post_reactions_haha_total",
                            "post_reactions_sorry_total",
                            "post_reactions_anger_total",
                            "post_reactions_care_total",
                        )
                        for fld in reaction_fields:
                            record[fld] = 0.0

                        # Insight key -> our field name. Facebook returns reaction
                        # types as keys inside post_reactions_by_type_total's value.
                        reaction_key_map = {
                            "like": "post_reactions_like_total",
                            "love": "post_reactions_love_total",
                            "wow": "post_reactions_wow_total",
                            "haha": "post_reactions_haha_total",
                            "sorry": "post_reactions_sorry_total",
                            "anger": "post_reactions_anger_total",
                            "care": "post_reactions_care_total",
                        }

                        try:
                            from shared.api_retry import (
                                with_exponential_backoff,
                                is_transient_error,
                            )

                            insights_fn = with_exponential_backoff(
                                resource_name=f"post_insights:{post_id}", max_retries=3
                            )(_fetch_post_insights)
                            insights = insights_fn()

                            for insight in insights:
                                insight_data = dict(insight)
                                metric_name = insight_data.get("name")
                                values_list = insight_data.get("values", [])
                                if not values_list:
                                    continue
                                value = values_list[0].get("value", 0)

                                if metric_name == "post_reactions_by_type_total":
                                    # Replacement for the 7 deprecated per-type metrics:
                                    # one insight call returns a dict like {'like': 31, 'love': 17, ...}
                                    if isinstance(value, dict):
                                        for rkey, count in value.items():
                                            field = reaction_key_map.get(rkey.lower())
                                            if not field:
                                                continue
                                            try:
                                                record[field] = (
                                                    float(count) if count else 0.0
                                                )
                                            except (ValueError, TypeError):
                                                record[field] = 0.0
                                    continue

                                try:
                                    record[metric_name] = float(value) if value else 0.0
                                except (ValueError, TypeError):
                                    record[metric_name] = 0.0
                        except Exception as e:
                            error_msg = str(e)
                            if "valid insights metric" in error_msg.lower():
                                logger.warning(
                                    f"    Insights rejected for post {post_id} -- check metric names in config: {error_msg}"
                                )
                            else:
                                logger.warning(
                                    f"    Insights error for post {post_id}: {error_msg}"
                                )
                                logger.debug(
                                    f"    Full error details: {repr(e)}", exc_info=True
                                )

                        records_extracted += 1
                        yield record

                    except Exception as e:
                        error_str = str(e).lower()
                        if any(
                            term in error_str
                            for term in [
                                "rate limit",
                                "too many requests",
                                "429",
                                "api call limit",
                                "throttled",
                            ]
                        ):
                            backoff_time = handle_rate_limit_error(
                                "facebook_post_insights", e
                            )
                            logger.warning(
                                f"  Rate limit hit for post {post_id}, backing off {backoff_time:.1f}s before retry"
                            )
                        else:
                            logger.warning(
                                f"  Failed to extract post data for {post_id}: {e}"
                            )
                        continue

    except Exception as e:
        logger.error(f"Error extracting post insights: {e}")
        import traceback

        logger.debug(traceback.format_exc())

    finally:
        # Restore original API configuration
        if original_access_token:
            FacebookAdsApi.init(
                original_app_id,
                original_app_secret,
                original_access_token,
                api_version="v25.0",
            )
            logger.debug(
                "Restored original API configuration after post_insights extraction"
            )


def extract_post_attachments(
    accounts: List[str],
    date_range: tuple = None,
    config: Dict = None,
    pages: List[Dict] = None,
    spool=None,
) -> Iterator[Dict]:
    """
    Extract post attachment metadata from Facebook (images, links, videos).
    Uses parent post spool to avoid duplicate post fetches.

    Args:
        accounts: List of Facebook account IDs
        date_range: Tuple of (start_date, end_date)
        config: Table configuration from YAML
        pages: Optional list of page dictionaries
        spool: Optional PostSpool instance

    Yields:
        Dict records with attachment data (unshimmed_url, media_type, title, description, etc.)
    """
    from facebook_business.adobjects.pagepost import PagePost

    # Save original API configuration to restore later
    original_api = FacebookAdsApi.get_default_api()
    original_app_id = getattr(original_api, "app_id", None) if original_api else None
    original_app_secret = (
        getattr(original_api, "app_secret", None) if original_api else None
    )
    original_access_token = (
        getattr(original_api, "access_token", None) if original_api else None
    )

    try:
        # Use spool if available, otherwise extract posts (post_attachments without posts table)
        if spool is None or spool.size() == 0:
            spool = get_current_spool() or spool

        # If spool is empty, extract posts first
        if not spool or spool.size() == 0:
            logger.info(
                "Post spool empty - extracting posts for post_attachments dependency"
            )
            # extract_posts populates the current global spool (creating one if needed);
            # never create_spool() here or a populated global gets clobbered
            _ = list(extract_posts(accounts, pages=pages, date_range=date_range))
            spool = get_current_spool() or spool

        if not spool or spool.size() == 0:
            logger.warning("No posts found to extract attachments from")
            return

        posts = list(spool.get_all_manifests())

        if not posts:
            logger.warning("No posts found to extract attachments from")
            return

        logger.info(f"Extracting attachments for {len(posts)} posts")

        # Process each post
        posts_processed = 0
        attachments_extracted = 0
        posts_with_attachments = 0
        failed_posts = []  # Track posts that permanently failed

        for post_data in posts:
            post_id = post_data["id"]
            account_id = post_data.get("account_id")
            page_id = post_data.get("page_id")
            page_name = post_data.get("page_name")
            created_time = post_data.get("created_time")
            page_access_token = post_data.get("page_access_token")

            if not page_access_token:
                logger.debug(
                    f"  Skipping post {post_id} - no page access token available"
                )
                continue

            posts_processed += 1
            if posts_processed % 100 == 0:
                logger.info(
                    f"  Progress: {posts_processed}/{len(posts)} posts processed, {attachments_extracted} attachments extracted"
                )

            logger.debug(f"  Extracting attachments for post: {post_id}")

            try:
                # Create temporary API instance with page-specific access token
                from facebook_business.session import FacebookSession

                temp_session = FacebookSession(
                    app_id=original_app_id,
                    app_secret=original_app_secret,
                    access_token=page_access_token,
                )
                temp_api = FacebookAdsApi(temp_session, api_version="v25.0")

                # Create PagePost object with temporary API
                post = PagePost(fbid=post_id, api=temp_api)

                # Request attachment fields based on CSV analysis recommendations
                attachment_fields = [
                    "url",
                    "unshimmed_url",
                    "media_type",
                    "type",
                    "title",
                    "description",
                    "media",
                    "target",
                ]

                # Call attachments API
                attachments_response = post.api_get(
                    fields=[f"attachments{{{','.join(attachment_fields)}}}"]
                )

                # Extract attachments from response
                attachments_data = attachments_response.get("attachments", {})
                if isinstance(attachments_data, dict):
                    attachments_list = attachments_data.get("data", [])
                else:
                    attachments_list = []

                if not attachments_list:
                    logger.debug(f"    No attachments found for post {post_id}")
                    continue

                posts_with_attachments += 1

                # Process each attachment (typically 1 per post)
                for attachment in attachments_list:
                    # Extract recommended fields based on CSV fill rate analysis
                    record = {
                        "account_id": account_id,
                        "post_id": post_id,
                        "page_id": page_id,
                        "page_name": page_name,
                        "created_time": created_time,
                        # Tier 1: Critical fields (100% filled)
                        "unshimmed_url": attachment.get("unshimmed_url"),
                        "media_type": attachment.get("media_type"),
                        "type": attachment.get("type"),
                        # Tier 2: High-value fields (95-99% filled)
                        "description": attachment.get("description"),
                        # Tier 3: Conditional fields
                        "title": attachment.get("title"),  # 30.7% - for shared links
                    }

                    # Extract media image fields if present (99.4% filled)
                    media = attachment.get("media")
                    if media and isinstance(media, dict):
                        image_data = media.get("image", {})
                        if isinstance(image_data, dict):
                            record["media_image_url"] = image_data.get("src")
                            record["media_image_width"] = image_data.get("width")
                            record["media_image_height"] = image_data.get("height")
                        else:
                            record["media_image_url"] = None
                            record["media_image_width"] = None
                            record["media_image_height"] = None
                    else:
                        record["media_image_url"] = None
                        record["media_image_width"] = None
                        record["media_image_height"] = None

                    # Extract target fields if present (72.3% filled)
                    target = attachment.get("target")
                    if target and isinstance(target, dict):
                        record["target_id"] = target.get("id")
                        record["target_url"] = target.get("url")
                    else:
                        record["target_id"] = None
                        record["target_url"] = None

                    attachments_extracted += 1
                    yield record

            except Exception as e:
                error_str = str(e)
                # Check if it's a "no data" error (post has no attachments)
                if (
                    "Unsupported get request" in error_str
                    or "does not exist" in error_str
                ):
                    logger.debug(f"    Post {post_id} has no attachments")
                # Check if it's a connection error that should be retried
                elif (
                    "Connection aborted" in error_str
                    or "ConnectionResetError" in error_str
                    or "forcibly closed" in error_str
                ):
                    # Retry connection errors up to 3 times with exponential backoff
                    retry_count = 0
                    max_retries = 3
                    retry_successful = False

                    while retry_count < max_retries:
                        retry_count += 1
                        wait_time = (
                            2**retry_count
                        )  # Exponential backoff: 2, 4, 8 seconds
                        logger.warning(
                            f"  Connection error for post {post_id}, retrying {retry_count}/{max_retries} after {wait_time}s..."
                        )
                        time.sleep(wait_time)

                        try:
                            # Recreate temporary API instance
                            from facebook_business.session import FacebookSession

                            temp_session = FacebookSession(
                                app_id=original_app_id,
                                app_secret=original_app_secret,
                                access_token=page_access_token,
                            )
                            temp_api = FacebookAdsApi(temp_session, api_version="v25.0")
                            post = PagePost(fbid=post_id, api=temp_api)

                            # Request attachment fields
                            attachment_fields = [
                                "url",
                                "unshimmed_url",
                                "media_type",
                                "type",
                                "title",
                                "description",
                                "media",
                                "target",
                            ]
                            attachments_response = post.api_get(
                                fields=[f"attachments{{{','.join(attachment_fields)}}}"]
                            )

                            # Extract attachments from response
                            attachments_data = attachments_response.get(
                                "attachments", {}
                            )
                            if isinstance(attachments_data, dict):
                                attachments_list = attachments_data.get("data", [])
                            else:
                                attachments_list = []

                            if attachments_list:
                                posts_with_attachments += 1
                                for attachment in attachments_list:
                                    record = {
                                        "account_id": account_id,
                                        "post_id": post_id,
                                        "page_id": page_id,
                                        "page_name": page_name,
                                        "created_time": created_time,
                                        "unshimmed_url": attachment.get(
                                            "unshimmed_url"
                                        ),
                                        "media_type": attachment.get("media_type"),
                                        "type": attachment.get("type"),
                                        "description": attachment.get("description"),
                                        "title": attachment.get("title"),
                                    }

                                    # Extract media image fields
                                    media = attachment.get("media")
                                    if media and isinstance(media, dict):
                                        image_data = media.get("image", {})
                                        if isinstance(image_data, dict):
                                            record["media_image_url"] = image_data.get(
                                                "src"
                                            )
                                            record["media_image_width"] = (
                                                image_data.get("width")
                                            )
                                            record["media_image_height"] = (
                                                image_data.get("height")
                                            )
                                        else:
                                            record["media_image_url"] = None
                                            record["media_image_width"] = None
                                            record["media_image_height"] = None
                                    else:
                                        record["media_image_url"] = None
                                        record["media_image_width"] = None
                                        record["media_image_height"] = None

                                    # Extract target fields
                                    target = attachment.get("target")
                                    if target and isinstance(target, dict):
                                        record["target_id"] = target.get("id")
                                        record["target_url"] = target.get("url")
                                    else:
                                        record["target_id"] = None
                                        record["target_url"] = None

                                    attachments_extracted += 1
                                    yield record

                            retry_successful = True
                            logger.info(f"  Retry successful for post {post_id}")
                            break

                        except Exception as retry_error:
                            if retry_count >= max_retries:
                                logger.error(
                                    f"  Failed to extract attachments for post {post_id} after {max_retries} retries: {retry_error}"
                                )
                            else:
                                logger.debug(
                                    f"  Retry {retry_count} failed: {retry_error}"
                                )

                    if not retry_successful:
                        logger.error(
                            f"  Permanently failed to extract attachments for post {post_id} after {max_retries} retries"
                        )
                        failed_posts.append(
                            {
                                "post_id": post_id,
                                "page_id": page_id,
                                "page_name": page_name,
                                "error": str(e),
                            }
                        )
                else:
                    logger.warning(
                        f"  Failed to extract attachments for post {post_id}: {e}"
                    )
                continue

        logger.info(
            f"Extraction complete: {attachments_extracted} attachments from {posts_with_attachments}/{posts_processed} posts"
        )

        # Report failed posts if any
        if failed_posts:
            # Get tenant_field from config (default to 'page_id' for posts)
            tenant_field = (
                config.get("tenant_field", "page_id") if config else "page_id"
            )
            tenant_name_field = tenant_field.replace("_id", "_name")

            logger.warning(
                f"\n{len(failed_posts)} post(s) permanently failed after retries:"
            )

            # Group by tenant (page_id for posts, account_id for ads, etc.)
            from collections import defaultdict

            failed_by_tenant = defaultdict(list)
            for failure in failed_posts:
                tenant_id = failure.get(tenant_field, "unknown")
                failed_by_tenant[tenant_id].append(failure["post_id"])

            # Show summary by tenant
            for tenant_id, post_ids in failed_by_tenant.items():
                tenant_name = next(
                    (
                        f.get(tenant_name_field, tenant_id)
                        for f in failed_posts
                        if f.get(tenant_field) == tenant_id
                    ),
                    tenant_id,
                )
                logger.warning(
                    f"  {tenant_name} ({tenant_id}): {len(post_ids)} failed posts"
                )
                for post_id in post_ids[:5]:  # Show first 5
                    logger.warning(f"    - {post_id}")
                if len(post_ids) > 5:
                    logger.warning(f"    ... and {len(post_ids) - 5} more")

            # Generate retry suggestion with tenant filter
            logger.info("\nRETRY OPTIONS:")
            logger.info("=" * 80)

            # Option 1: Retry all (re-run same command)
            lookback_days = (date_range[1] - date_range[0]).days if date_range else 7
            logger.info(
                "1. Retry ALL pages (recommended - automatic retry will catch failures):"
            )
            logger.info(
                f"   python orchestrate.py --source facebook --env prod --tables post_attachments --lookback {lookback_days}"
            )

            # Option 2: Retry specific tenants (pages with failures)
            if len(failed_by_tenant) <= 5:  # Only show if manageable number
                logger.info("\n2. Retry ONLY failed pages:")
                for tenant_id in list(failed_by_tenant.keys()):
                    logger.info(
                        f"   python orchestrate.py --source facebook --env prod --tables post_attachments --sites {tenant_id} --lookback {lookback_days}"
                    )
            else:
                # Too many to list individually
                tenant_list = " ".join(list(failed_by_tenant.keys())[:10])
                logger.info(
                    f"\n2. Retry ONLY failed pages (showing first 10 of {len(failed_by_tenant)}):"
                )
                logger.info(
                    f"   python orchestrate.py --source facebook --env prod --tables post_attachments --sites {tenant_list} --lookback {lookback_days}"
                )

            logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Error extracting post attachments: {e}")
        import traceback

        logger.debug(traceback.format_exc())

    finally:
        # Restore original API configuration
        if original_access_token:
            FacebookAdsApi.init(
                original_app_id,
                original_app_secret,
                original_access_token,
                api_version="v25.0",
            )
            logger.debug(
                "Restored original API configuration after post_attachments extraction"
            )


def run_pipeline(
    config: Dict[str, Any],
    sites: List[str],
    tables: List[str],
    group: Optional[str] = None,
    refresh_mode: str = "incremental",
    lookback_days: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    test_mode: bool = False,
    batch_size: Optional[int] = None,
    max_retries: int = 3,
    skip_validation: bool = False,
    export_format: Optional[str] = None,
    export_dir: Optional[str] = None,
    skip_hash_merge: bool = False,
    archive_staging: bool = False,
    truncate_staging: bool = False,
    rebuild: bool = False,
    bq_client: Any = None,
    execution_id: str = None,
    parallel_workers: Optional[int] = None,
    schema_prefix: Optional[str] = None,
    schema_suffix: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:

    from dotenv import load_dotenv
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import functools

    load_dotenv()

    # Rate limiting configuration
    RATE_LIMIT_DELAY = 2  # seconds between account extractions
    BATCH_DELAY = 0.5  # seconds between batches

    # Set default parallel workers if not provided
    if parallel_workers is None:
        parallel_workers = config.get("optimization", {}).get("max_parallel_workers", 3)

    # Initialize Facebook API
    if not initialize_api(config):
        raise Exception("Failed to initialize Facebook API")

    # Handle site/account specification
    if not sites or sites == [] or sites == [""] or sites == [None]:
        account_id = os.getenv("FACEBOOK_ACCOUNT_ID")
        if account_id:
            sites = (
                [aid.strip() for aid in account_id.split(",")]
                if "," in account_id
                else [account_id]
            )
            sites = [s if s.startswith("act_") else f"act_{s}" for s in sites]
            logger.info(f"Using Facebook account(s) from environment: {sites}")
        else:
            logger.error("No FACEBOOK_ACCOUNT_ID found in environment")
            return {"total_rows": 0, "sites": 0, "tables": 0}

    elif sites == ["all"]:
        account_id = os.getenv("FACEBOOK_ACCOUNT_ID")
        if account_id:
            sites = (
                [aid.strip() for aid in account_id.split(",")]
                if "," in account_id
                else [account_id]
            )
            sites = [s if s.startswith("act_") else f"act_{s}" for s in sites]
            logger.info(f"Using all Facebook account(s) from environment: {sites}")
        else:
            sites = get_available_sites(config)
            if not sites:
                logger.error("No accounts found via API or environment")
                return {"total_rows": 0, "sites": 0, "tables": 0}
            logger.info(f"Discovered {len(sites)} accounts via API")
    else:
        sites = [s if s.startswith("act_") else f"act_{s}" for s in sites]
        logger.info(f"Using specified Facebook account(s): {sites}")

    # Get tables if not specified
    if not tables:
        tables = get_available_tables(config, group)
        if not tables:
            logger.error("No tables specified or found in group")
            return {"total_rows": 0, "sites": 0, "tables": 0}

    # Pacing: process heavy insights tables LAST so lighter, higher-value tables
    # finish before the app request budget tightens. Order within each group is
    # otherwise preserved (stable sort). Configurable via extractor.insights_pacing.
    _pacing_config = config.get("extractor", {}).get("insights_pacing", {})
    _heavy_tables = set(_pacing_config.get("heavy_tables", []))
    inter_table_delay = _pacing_config.get("inter_table_delay_seconds", 0)
    heavy_table_delay = _pacing_config.get("heavy_table_delay_seconds", 0)
    if _heavy_tables and tables:
        tables = sorted(tables, key=lambda t: 1 if t in _heavy_tables else 0)
        logger.info(
            f"Pacing enabled: heavy tables {sorted(_heavy_tables & set(tables))} ordered last"
        )

    # Spool dependency: posts must run before post_insights/post_attachments so
    # dependents reuse spooled posts instead of re-fetching every page.
    _post_dependents = [t for t in tables if t in ("post_insights", "post_attachments")]
    if "posts" in tables and _post_dependents:
        tables = list(tables)
        first_dep_idx = min(tables.index(t) for t in _post_dependents)
        if tables.index("posts") > first_dep_idx:
            tables.remove("posts")
            tables.insert(first_dep_idx, "posts")
            logger.info(
                "Spool ordering: posts moved before post_insights/post_attachments"
            )

    logger.info(f"Extracting {len(tables)} tables from {len(sites)} accounts")

    # Parse date range -
    if start_date and end_date:
        # Parse date strings using centralized utility
        start_dt = (
            parse_facebook_timestamp(start_date)
            if isinstance(start_date, str)
            else start_date
        )
        end_dt = (
            parse_facebook_timestamp(end_date)
            if isinstance(end_date, str)
            else end_date
        )
        date_range = (
            start_dt.date() if isinstance(start_dt, datetime) else start_dt,
            end_dt.date() if isinstance(end_dt, datetime) else end_dt,
        )
    elif refresh_mode == "full":
        # FULL REFRESH - Get maximum historical data
        end_dt = date.today()

        # Get max_months from config, default to 37 (Facebook's limit)
        max_months = (
            config.get("extractor", {}).get("full_refresh", {}).get("max_months", 37)
        )

        # Calculate start date based on max_months
        start_dt = end_dt - timedelta(days=max_months * 30)
        date_range = (start_dt, end_dt)

        logger.info(f"FULL REFRESH MODE: Extracting {max_months} months of data")
        logger.info(f"Full date range: {start_dt} to {end_dt}")
    else:
        # Regular extraction with lookback
        end_dt = date.today()

        # Get lookback from parameter, config, or default
        if lookback_days:
            days_back = lookback_days
        else:
            days_back = config.get("extractor", {}).get("lookback_days", 30)

        start_dt = end_dt - timedelta(days=days_back)
        date_range = (start_dt, end_dt)

    logger.info(f"Date range: {date_range[0]} to {date_range[1]}")

    # Check for parent-child table relationships and inform user
    table_dependencies = config.get("pipeline", {}).get("table_dependencies", {})
    child_tables_to_extract = []

    for table in tables:
        # Check if this table will extract child tables
        for child_table, dep_info in table_dependencies.items():
            if dep_info.get("extracted_via") == table:
                child_tables_to_extract.append(child_table)

    if child_tables_to_extract:
        logger.info(f"")
        logger.info(
            f"NOTE: The following additional tables will be automatically extracted:"
        )
        for child_table in child_tables_to_extract:
            dep_info = table_dependencies[child_table]
            parent = dep_info.get("extracted_via")
            reason = dep_info.get("reason", "API limitation")
            logger.info(f"  - {child_table} (via {parent}): {reason}")
        logger.info(
            f"Total tables to be created: {len(tables) + len(child_tables_to_extract)}"
        )
        logger.info(f"")

    # Initialize shared pipeline environment (datasets, naming, metadata)
    env = initialize_pipeline_environment(
        config,
        bq_client=bq_client,
        source_default="facebook",
        schema_prefix=schema_prefix,
        schema_suffix=schema_suffix,
        execution_id=execution_id,
        rebuild=rebuild,
        require_bucket=False,
        default_production_suffix="_test",
    )

    source = env.source_name
    staging_dataset = env.staging_dataset
    main_dataset = env.production_dataset
    run_execution_id = env.execution_id or execution_id
    exec_id = run_execution_id or execution_id

    total_rows = 0
    # B1: standardized per-table accounting + return shape via the shared
    # accumulator (shared/extraction_result). Facebook stamps metadata and shapes
    # records itself (per-handler) before calling extract_to_local_parquet, so it
    # records the already-written Parquet via record_prewritten -- same pattern as
    # instagram/dcm. The authoritative per-table TABLE_STATUS_* verdict the
    # orchestrator consumes is produced by record_prewritten/skip_table/
    # fail_table/rate_limited_table. rate_limit_hits stays a local counter and is
    # carried through finalize(**extra).
    #
    # VERIFICATION STATUS (2026-06): unit-green (full suite + the 9
    # test_facebook_table_status contract tests pass), but NOT yet live-verified
    # (the facebook_business SDK is absent in the dev env, so run_pipeline cannot
    # execute here). Before trusting in production, run a bounded live extraction
    # on a small group and confirm: table_files keyed by logical name, per-table
    # table_status matches the legacy verdict (success/zero_rows/rate_limited/
    # failed), and the merge row counts match a flag-equivalent baseline.
    from shared.extraction_result import StandardExtractionResult

    result = StandardExtractionResult(
        source="facebook",
        execution_id=exec_id,
        job_id=exec_id,
        bq_client=bq_client,
        sites=sites,
    )
    rate_limit_hits = 0

    # Discover pages once up-front whenever any page-dependent table is being extracted.
    # This ensures new pages are always picked up at the start of every run, and avoids
    # redundant API calls when multiple page-dependent tables run in the same pipeline.
    PAGE_DEPENDENT_TABLES = {
        "pages",
        "posts",
        "page_insights",
        "post_insights",
        "post_attachments",
    }
    _cached_pages: Optional[List[Dict]] = None
    if PAGE_DEPENDENT_TABLES & set(tables):
        logger.info("Discovering Facebook pages before extraction...")
        _cached_pages = list(extract_pages(sites))
        logger.info(
            f"Found {len(_cached_pages)} pages: {[p.get('name', p.get('id')) for p in _cached_pages]}"
        )
        if not _cached_pages:
            logger.warning(
                "No Facebook pages found -- page-dependent tables will be skipped"
            )

    # Initialize post spool for per-run parent extraction pattern
    # This eliminates duplicate post fetches when extracting posts, post_insights, and post_attachments
    if "posts" in tables:
        spool = create_spool()
        logger.info("Created post spool for this extraction run")
    else:
        spool = None

    for table_index, table in enumerate(tables):
        # Check if table is active before processing
        table_config = config.get("resources", {}).get(table, {})
        if not table_config.get("active", True):
            logger.info(f"Table '{table}' is marked as active: false, skipping")
            continue

        # Pacing pause between tables (skip before the very first table). Heavy
        # insights tables get a longer cooldown to let the request budget recover.
        if table_index > 0:
            delay = heavy_table_delay if table in _heavy_tables else inter_table_delay
            if delay and delay > 0:
                logger.info(f"  Pacing: waiting {delay}s before {table}")
                time.sleep(delay)

        logger.info(f"\n\n\nProcessing table: {table}")

        # Apply schema prefix/suffix to table name
        table_with_affix = apply_table_affix(table, schema_prefix, schema_suffix)

        all_data = []
        table_success = False

        # Route to appropriate extraction based on table type
        # Note: adcreatives are extracted via ads table (parent object pattern)
        if table in ["campaigns", "adsets", "ads", "adimages", "customaudiences"]:
            # Skip direct extraction of adcreatives - they're extracted from ads
            # Define function to process a single account
            @with_rate_limit(
                source="facebook_account_processing", calls_per_second=0.03
            )  # ~100 calls per hour
            def process_single_account(account_id, table_name, test_mode):
                retry_count = 0
                max_account_retries = 3

                while retry_count <= max_account_retries:
                    try:
                        # Apply resource-specific rate limit
                        wait_for_facebook(table_name)

                        # Get table config for incremental date fields
                        table_config = config.get("resources", {}).get(table_name, {})

                        # Pass date_range, rebuild, and table_config for smart incremental extraction
                        records = list(
                            extract_hierarchy(
                                [account_id],
                                table_name,
                                test_mode,
                                date_range=date_range,
                                rebuild_mode=rebuild,
                                table_config=table_config,
                            )
                        )
                        logger.info(f"  {account_id}: {len(records)} {table_name}")

                        # Reset rate limit on success
                        reset_rate_limit(f"facebook_{table_name}")
                        return records
                    except Exception as e:
                        if (
                            "User request limit reached" in str(e)
                            or "Too Many Calls" in str(e)
                            or "rate limit" in str(e).lower()
                        ):
                            retry_count += 1
                            # Use our enhanced rate limiter for backoff
                            backoff_time = handle_rate_limit_error(
                                f"facebook_{table_name}", e
                            )

                            if retry_count <= max_account_retries:
                                logger.warning(
                                    f"  Rate limit hit. Waiting {backoff_time:.1f}s before retry {retry_count}/{max_account_retries}"
                                )
                                time.sleep(backoff_time)
                            else:
                                logger.error(
                                    f"  {account_id}: Max retries exceeded after rate limit"
                                )
                                return []
                        else:
                            logger.error(f"  {account_id}: Failed - {e}")
                            return []
                return []

            # Use parallel processing if we have multiple accounts and parallel workers > 1
            if len(sites) > 1 and parallel_workers > 1 and not test_mode:
                logger.info(
                    f"  Processing {len(sites)} accounts in parallel with {parallel_workers} workers"
                )
                with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                    # Create tasks for each account
                    futures = {
                        executor.submit(
                            process_single_account, account_id, table, test_mode
                        ): account_id
                        for account_id in sites
                    }

                    # Process results as they complete
                    for future in as_completed(futures):
                        account_id = futures[future]
                        try:
                            records = future.result()
                            all_data.extend(records)
                            rate_limit_hits += 1 if not records else 0
                        except Exception as e:
                            logger.error(f"  {account_id}: Unexpected error - {e}")
                            rate_limit_hits += 1
            else:
                # Sequential processing for single account or test mode
                for i, account_id in enumerate(sites):
                    # Add delay between accounts to avoid rate limits
                    if i > 0:
                        logger.info(
                            f"  Rate limit prevention delay: {RATE_LIMIT_DELAY}s"
                        )
                        time.sleep(RATE_LIMIT_DELAY)

                    records = process_single_account(account_id, table, test_mode)
                    all_data.extend(records)
                    if not records:
                        rate_limit_hits += 1

            # WORDPRESS PATTERN: After extraction completes, save to Parquet
            if all_data:
                # SPECIAL HANDLING: If this is ads table, extract creatives first
                if table == "ads":
                    creatives_data = []
                    seen_creative_ids = set()

                    for ad_record in all_data:
                        # Extract creative object from ad (stored before JSON serialization)
                        creative_obj = ad_record.get("_creative_obj")
                        if creative_obj and isinstance(creative_obj, dict):
                            creative_id = creative_obj.get("id")
                            if creative_id and creative_id not in seen_creative_ids:
                                seen_creative_ids.add(creative_id)

                                # Check if creative_obj only has ID (nested expansion no longer works in Graph API v25.0+)
                                # If so, fetch full creative details via AdCreative endpoint
                                if creative_obj.get("name") is None and creative_id:
                                    try:
                                        from facebook_business.adobjects.adcreative import (
                                            AdCreative,
                                        )

                                        creative_api = AdCreative(creative_id)
                                        creative_details = creative_api.api_get(
                                            fields=[
                                                "id",
                                                "name",
                                                "status",
                                                "body",
                                                "title",
                                                "link_url",
                                                "call_to_action_type",
                                                "object_type",
                                                "object_story_spec",
                                            ]
                                        )
                                        # Update creative_obj with fetched details
                                        creative_obj = dict(creative_details)
                                        logger.debug(
                                            f"    Fetched creative details for {creative_id}"
                                        )
                                    except Exception as e:
                                        logger.debug(
                                            f"    Could not fetch creative {creative_id}: {e}"
                                        )
                                        # Continue with partial data

                                # Create creative record
                                creative_record = {
                                    "id": creative_id,
                                    "account_id": ad_record.get("account_id"),
                                    "name": creative_obj.get("name"),
                                    "status": creative_obj.get("status"),
                                    "body": creative_obj.get("body"),
                                    "title": creative_obj.get("title"),
                                    "link_url": creative_obj.get("link_url"),
                                    "call_to_action_type": creative_obj.get(
                                        "call_to_action_type"
                                    ),
                                    "object_type": creative_obj.get("object_type"),
                                    "object_story_spec": creative_obj.get(
                                        "object_story_spec"
                                    ),
                                    "extracted_at": ad_record.get(
                                        "extracted_at"
                                    ),  # Already datetime
                                    "execution_id": exec_id,
                                    "source": source,
                                }

                                # Add row hash
                                creative_record.update(
                                    ensure_string_ids(creative_record)
                                )
                                for key, value in list(creative_record.items()):
                                    if isinstance(value, (dict, list)):
                                        creative_record[key] = json.dumps(value)

                                creatives_data.append(creative_record)

                        # Remove temp field before saving ads
                        if "_creative_obj" in ad_record:
                            del ad_record["_creative_obj"]

                    # Save creatives to separate table if any were found
                    if creatives_data:
                        logger.info(
                            f"  Extracted {len(creatives_data)} unique ad creatives from ads"
                        )

                        # Get adcreatives table config
                        creatives_table_config = config.get("resources", {}).get(
                            "adcreatives", {}
                        )
                        creatives_base_table = creatives_table_config.get(
                            "table_name", "facebook_adcreatives"
                        )
                        creatives_table_with_affix = apply_table_affix(
                            creatives_base_table, schema_prefix, schema_suffix
                        )
                        creatives_production_table_id = (
                            f"{env.project}.{main_dataset}.{creatives_table_with_affix}"
                        )

                        # Save creatives to Parquet
                        creatives_file_path = extract_to_local_parquet(
                            data=creatives_data,
                            source="facebook",
                            table="adcreatives",
                            job_id=exec_id,
                            bq_client=bq_client,
                            production_table_id=creatives_production_table_id,
                            rebuild_mode=rebuild,
                        )

                        if creatives_file_path:
                            result.record_prewritten(
                                "adcreatives",
                                creatives_file_path,
                                len(creatives_data),
                            )
                            logger.info(
                                f"  Ad creatives Parquet file created: {creatives_file_path}"
                            )

                # Add metadata to all records
                set_execution_metadata(all_data, exec_id, source=source)
                for record in all_data:
                    record.update(ensure_string_ids(record))
                    for key, value in list(record.items()):
                        if isinstance(value, (dict, list)):
                            record[key] = json.dumps(value)

                # Get table config and production table ID
                table_config = config.get("resources", {}).get(table, {})
                base_table_name = table_config.get("table_name", f"{source}_{table}")
                table_name_with_affix = apply_table_affix(
                    base_table_name, schema_prefix, schema_suffix
                )
                production_table_id = (
                    f"{env.project}.{main_dataset}.{table_name_with_affix}"
                )

                # Extract to local Parquet file
                local_file_path = extract_to_local_parquet(
                    data=all_data,
                    source="facebook",
                    table=table,
                    job_id=exec_id,
                    bq_client=bq_client,
                    production_table_id=production_table_id,
                    rebuild_mode=rebuild,
                )

                if local_file_path:
                    result.record_prewritten(table, local_file_path, len(all_data))
                    table_success = True
                    logger.info(f"  Parquet file created: {local_file_path}")
                else:
                    logger.info(
                        f"  No Parquet file created (zero rows - normal for incremental run)"
                    )
                    result.skip_table(table, reason="zero_rows")
                    table_success = True

                # Clear all_data to prevent old BigQuery loading
                all_data = []
            else:
                logger.info(
                    f"  No data extracted for {table} (zero rows - normal for incremental run)"
                )
                result.skip_table(table, reason="zero_rows")
                table_success = True

        elif table in [
            "account_insights",
            "campaign_insights",
            "adset_insights",
            "ad_insights",
            "ad_insights_actions",
        ]:
            # ASYNC insights extraction for ad-level insights ONLY
            # NOTE: page_insights and post_insights use different extraction (page_related pattern below)
            table_config = config.get("resources", {}).get(table, {})

            # Determine level from table name
            if "account" in table:
                level = "account"
            elif "campaign" in table:
                level = "campaign"
            elif "adset" in table:
                level = "adset"
            else:
                level = "ad"

            # Note: Do NOT override fields here - let extract_insights_async use its comprehensive defaults
            # which include all ID fields (account_id, campaign_id, adset_id, ad_id) and name fields

            table_had_rate_limit = False
            table_had_hard_failure = False

            for i, account_id in enumerate(sites):
                if i > 0:
                    time.sleep(RATE_LIMIT_DELAY)

                try:
                    records = list(
                        extract_insights_async(
                            account_id, table_config, date_range, level, test_mode
                        )
                    )
                    all_data.extend(records)
                    logger.info(
                        f"  {account_id}: {len(records)} {level} insight records"
                    )
                except Exception as e:
                    if is_facebook_rate_limit(e):
                        rate_limit_hits += 1
                        # Recoverable: the next incremental run backfills this
                        # window. Record rate_limited (not failed) so it does
                        # not raise a false alert, and do not also count it
                        # successful below.
                        table_had_rate_limit = True
                        logger.warning(
                            f"  {account_id}: Rate limited, will retry in next run"
                        )
                    else:
                        logger.error(f"  {account_id}: Failed - {e}")
                        table_had_hard_failure = True
                        result.fail_table(
                            table, error=str(e), error_type=type(e).__name__
                        )
                        # Re-raise exception for critical failures (all jobs failed)
                        # This ensures pipeline fails instead of silently continuing
                        if "All" in str(e) and "jobs failed" in str(e):
                            raise

            # PARQUET/GCS PATTERN: Convert insights to Parquet after extraction
            if all_data:
                logger.info(f"  Extracted {len(all_data)} total {table} records")

                # Add metadata and normalize timestamps for all records
                timestamp_fields = ["date_start", "date_stop", "extracted_at"]
                set_execution_metadata(all_data, exec_id, source=source)
                for record in all_data:
                    record.update(ensure_string_ids(record))
                    # Parse timestamp fields using centralized utility
                    for field in timestamp_fields:
                        if field in record and record[field] is not None:
                            record[field] = parse_facebook_timestamp(record[field])
                    # Serialize complex fields
                    for key, value in list(record.items()):
                        if isinstance(value, (dict, list)):
                            record[key] = json.dumps(value)

                # Get table config and production table ID
                table_config = config.get("resources", {}).get(table, {})
                base_table_name = table_config.get("table_name", f"{source}_{table}")
                table_name_with_affix = apply_table_affix(
                    base_table_name, schema_prefix, schema_suffix
                )
                production_table_id = (
                    f"{env.project}.{main_dataset}.{table_name_with_affix}"
                )

                # Extract to local Parquet file with DuckDB schema discovery
                local_file_path = extract_to_local_parquet(
                    data=all_data,
                    source="facebook",
                    table=table,
                    job_id=exec_id,
                    bq_client=bq_client,
                    production_table_id=production_table_id,
                    rebuild_mode=rebuild,
                )

                if local_file_path:
                    result.record_prewritten(table, local_file_path, len(all_data))
                    logger.info(f"  Parquet file created: {local_file_path}")
                else:
                    logger.info(
                        f"  No Parquet file created (zero rows - normal for incremental run)"
                    )
                    result.skip_table(table, reason="zero_rows")

                # Clear all_data to prevent old BigQuery loading
                all_data = []
            else:
                # No rows. Distinguish a genuine empty incremental window from a
                # rate-limit or hard failure so the orchestrator does not alert
                # on the recoverable cases.
                if table_had_hard_failure:
                    # fail_table was already recorded in the per-account except
                    # block above (status FAILED, failed_tables bumped); status
                    # stands.
                    logger.warning(
                        f"  No data extracted for {table} (extraction failed - see error above)"
                    )
                elif table_had_rate_limit:
                    result.rate_limited_table(table)
                    logger.info(
                        f"  No data extracted for {table} (rate limited - next run will backfill)"
                    )
                else:
                    result.skip_table(table, reason="zero_rows")
                    logger.info(
                        f"  No data extracted for {table} (zero rows - normal for incremental run)"
                    )

        elif table == "leads":
            try:
                time.sleep(BATCH_DELAY)
                logger.info(f"Extracting leads")
                records = list(extract_leads(sites))
                logger.info(f"  Found {len(records)} leads")

                # Convert to Parquet/GCS pattern
                if records:
                    # Add metadata and ensure consistent types
                    set_execution_metadata(records, exec_id, source=source)
                    for record in records:
                        # Change extracted_at from isoformat string to datetime
                        if "extracted_at" in record and isinstance(
                            record["extracted_at"], str
                        ):
                            record["extracted_at"] = datetime.now()
                        record.update(ensure_string_ids(record))
                        # Serialize complex fields
                        for key, value in list(record.items()):
                            if isinstance(value, (dict, list)):
                                record[key] = json.dumps(value)

                    # Get table config and production table ID
                    table_config = config.get("resources", {}).get(table, {})
                    base_table_name = table_config.get(
                        "table_name", f"{source}_{table}"
                    )
                    table_name_with_affix = apply_table_affix(
                        base_table_name, schema_prefix, schema_suffix
                    )
                    production_table_id = (
                        f"{env.project}.{main_dataset}.{table_name_with_affix}"
                    )

                    # Extract to local Parquet file
                    local_file_path = extract_to_local_parquet(
                        data=records,
                        source="facebook",
                        table=table,
                        job_id=exec_id,
                        bq_client=bq_client,
                        production_table_id=production_table_id,
                        rebuild_mode=rebuild,
                    )
                    if local_file_path:
                        result.record_prewritten(table, local_file_path, len(records))
                        logger.info(f"  Parquet file created: {local_file_path}")
                    else:
                        logger.info(
                            f"  No Parquet file created (zero rows - normal for incremental run)"
                        )
                        result.skip_table(table, reason="zero_rows")

            except Exception as e:
                logger.error(f"  Leads extraction failed: {e}")
                result.fail_table(table, error=str(e), error_type=type(e).__name__)

        elif table == "ad_page_mapping":
            try:
                time.sleep(BATCH_DELAY)
                logger.info(f"Extracting ad-page mappings")
                records = list(extract_ad_page_mapping(sites))
                logger.info(f"  Found {len(records)} ad-page mappings")

                # Convert to Parquet/GCS pattern
                if records:
                    # Add metadata and ensure consistent types
                    set_execution_metadata(records, exec_id, source=source)
                    for record in records:
                        # Change extracted_at from isoformat string to datetime
                        if "extracted_at" in record and isinstance(
                            record["extracted_at"], str
                        ):
                            record["extracted_at"] = datetime.now()
                        record.update(ensure_string_ids(record))
                        # Serialize complex fields
                        for key, value in list(record.items()):
                            if isinstance(value, (dict, list)):
                                record[key] = json.dumps(value)

                    # Get table config and production table ID
                    table_config = config.get("resources", {}).get(table, {})
                    base_table_name = table_config.get(
                        "table_name", f"{source}_{table}"
                    )
                    table_name_with_affix = apply_table_affix(
                        base_table_name, schema_prefix, schema_suffix
                    )
                    production_table_id = (
                        f"{env.project}.{main_dataset}.{table_name_with_affix}"
                    )

                    # Extract to local Parquet file
                    local_file_path = extract_to_local_parquet(
                        data=records,
                        source="facebook",
                        table=table,
                        job_id=exec_id,
                        bq_client=bq_client,
                        production_table_id=production_table_id,
                        rebuild_mode=rebuild,
                    )
                    if local_file_path:
                        result.record_prewritten(table, local_file_path, len(records))
                        logger.info(f"  Parquet file created: {local_file_path}")
                    else:
                        logger.info(
                            f"  No Parquet file created (zero rows - normal for incremental run)"
                        )
                        result.skip_table(table, reason="zero_rows")

            except Exception as e:
                logger.error(f"  Ad-page mapping failed: {e}")
                result.fail_table(table, error=str(e), error_type=type(e).__name__)

        elif table == "adaccounts":
            # Extract account metadata - one record per account
            try:
                time.sleep(BATCH_DELAY)
                for account_id in sites:
                    if not account_id.startswith("act_"):
                        account_id = f"act_{account_id}"

                    wait_for_facebook("adaccounts")
                    account = AdAccount(account_id)
                    res_config = RESOURCE_CONFIG["adaccounts"]

                    # Get account data using api_get
                    account_data = account.api_get(fields=res_config["fields"])
                    record = dict(account_data)
                    record["account_id"] = account_id
                    record["extracted_at"] = datetime.now()

                    # Serialize Facebook SDK objects
                    for field, value in list(record.items()):
                        if value is not None and not isinstance(
                            value, (str, int, float, bool, list, dict, type(None))
                        ):
                            serialized = serialize_facebook_object(value)
                            if isinstance(serialized, (dict, list)):
                                record[field] = json.dumps(serialized)
                            else:
                                record[field] = serialized

                    all_data.append(record)

                logger.info(f"  Found {len(all_data)} account records")

                # Export to Parquet
                if all_data:
                    # Add metadata to all records
                    set_execution_metadata(all_data, exec_id, source=source)
                    for record in all_data:
                        # Ensure string IDs
                        record.update(ensure_string_ids(record))

                    # Get table config and production table ID for schema enforcement
                    table_config = config.get("resources", {}).get(table, {})
                    base_table_name = table_config.get(
                        "table_name", f"{source}_{table}"
                    )
                    table_name_with_affix = apply_table_affix(
                        base_table_name, schema_prefix, schema_suffix
                    )
                    production_table_id = (
                        f"{env.project}.{main_dataset}.{table_name_with_affix}"
                    )

                    local_file_path = extract_to_local_parquet(
                        data=all_data,
                        source="facebook",
                        table=table,
                        job_id=exec_id,
                        bq_client=bq_client,
                        production_table_id=production_table_id,
                        rebuild_mode=rebuild,
                    )

                    if local_file_path:
                        result.record_prewritten(table, local_file_path, len(all_data))
                        table_success = True
                        logger.info(f"  Parquet file created: {local_file_path}")
                else:
                    logger.warning(f"  No adaccount data extracted")
                    result.fail_table(table, error="No adaccount data extracted")

            except Exception as e:
                logger.error(f"  Adaccounts extraction failed: {e}")
                result.fail_table(table, error=str(e), error_type=type(e).__name__)

        elif table == "adcreatives":
            # Ad creatives are extracted via ads table (parent object pattern)
            # This avoids Facebook API limitations - adcreatives endpoint doesn't support date filtering
            logger.info(f"Ad creatives are extracted automatically via ads table")
            logger.info(f"  To extract creatives, run: --tables ads")
            # Skip to next table
            continue

        elif table == "pages":
            try:
                time.sleep(BATCH_DELAY)
                logger.info(f"Extracting pages")
                records = (
                    _cached_pages
                    if _cached_pages is not None
                    else list(extract_pages(sites))
                )
                logger.info(f"  Found {len(records)} pages")

                # WORDPRESS PATTERN: Extract to local Parquet instead of direct BigQuery
                if records:
                    # Add metadata to all records
                    set_execution_metadata(records, exec_id, source=source)
                    for record in records:
                        # Ensure string IDs
                        record.update(ensure_string_ids(record))
                        # Serialize complex fields
                        for key, value in list(record.items()):
                            if isinstance(value, (dict, list)):
                                record[key] = json.dumps(value)

                    # Get table config and production table ID for schema enforcement
                    table_config = config.get("resources", {}).get(table, {})
                    base_table_name = table_config.get(
                        "table_name", f"{source}_{table}"
                    )
                    table_name_with_affix = apply_table_affix(
                        base_table_name, schema_prefix, schema_suffix
                    )
                    production_table_id = (
                        f"{env.project}.{main_dataset}.{table_name_with_affix}"
                    )

                    # Extract to local Parquet file
                    local_file_path = extract_to_local_parquet(
                        data=records,
                        source="facebook",
                        table=table,
                        job_id=exec_id,
                        bq_client=bq_client,
                        production_table_id=production_table_id,
                        rebuild_mode=rebuild,
                    )

                    if local_file_path:
                        # Track successful extraction
                        result.record_prewritten(table, local_file_path, len(records))
                        table_success = True
                        logger.info(f"  Parquet file created: {local_file_path}")
                    else:
                        logger.info(
                            f"  No Parquet file created (zero rows - normal for incremental run)"
                        )
                        result.skip_table(table, reason="zero_rows")
                        table_success = True
                else:
                    logger.info(
                        f"  No data extracted for pages (zero rows - normal for incremental run)"
                    )
                    result.skip_table(table, reason="zero_rows")
                    table_success = True

            except Exception as e:
                logger.error(f"  Pages extraction failed: {e}")
                result.fail_table(table, error=str(e), error_type=type(e).__name__)

        elif table == "posts":
            try:
                time.sleep(BATCH_DELAY)
                logger.info(f"Extracting posts")
                records = list(
                    extract_posts(sites, pages=_cached_pages, date_range=date_range)
                )
                logger.info(f"  Found {len(records)} posts")

                # PARQUET/GCS PATTERN: Extract to local Parquet instead of direct BigQuery
                if records:
                    # Add metadata to all records and normalize timestamps
                    timestamp_fields = [
                        "created_time",
                        "updated_time",
                        "scheduled_publish_time",
                    ]
                    set_execution_metadata(records, exec_id, source=source)
                    for record in records:
                        # Normalize timestamp fields to catch invalid/out-of-range dates
                        for field in timestamp_fields:
                            if field in record and record[field] is not None:
                                # Parse timestamp using centralized utility (returns datetime directly)
                                record[field] = parse_facebook_timestamp(record[field])

                        # Ensure string IDs
                        record.update(ensure_string_ids(record))
                        # Serialize complex fields
                        for key, value in list(record.items()):
                            if isinstance(value, (dict, list)):
                                record[key] = json.dumps(value)

                    # Get table config and production table ID for schema enforcement
                    table_config = config.get("resources", {}).get(table, {})
                    base_table_name = table_config.get(
                        "table_name", f"{source}_{table}"
                    )
                    table_name_with_affix = apply_table_affix(
                        base_table_name, schema_prefix, schema_suffix
                    )
                    production_table_id = (
                        f"{env.project}.{main_dataset}.{table_name_with_affix}"
                    )

                    # Extract to local Parquet file
                    local_file_path = extract_to_local_parquet(
                        data=records,
                        source="facebook",
                        table=table,
                        job_id=exec_id,
                        bq_client=bq_client,
                        production_table_id=production_table_id,
                        rebuild_mode=rebuild,
                    )

                    if local_file_path:
                        # Track successful extraction
                        result.record_prewritten(table, local_file_path, len(records))
                        table_success = True
                        logger.info(f"  Parquet file created: {local_file_path}")
                    else:
                        logger.info(
                            f"  No Parquet file created (zero rows - normal for incremental run)"
                        )
                        result.skip_table(table, reason="zero_rows")
                        table_success = True
                else:
                    logger.info(
                        f"  No data extracted for posts (zero rows - normal for incremental run)"
                    )
                    result.skip_table(table, reason="zero_rows")
                    table_success = True

            except Exception as e:
                logger.error(f"  Posts extraction failed: {e}")
                result.fail_table(table, error=str(e), error_type=type(e).__name__)

        elif table == "page_insights":
            try:
                time.sleep(BATCH_DELAY)
                logger.info(f"Extracting page_insights")
                table_config = config.get("resources", {}).get(table, {})
                records = list(
                    extract_page_insights(
                        sites, date_range, table_config, pages=_cached_pages
                    )
                )
                logger.info(f"  Found {len(records)} page insight records")
                all_data.extend(records)

                if records:
                    # Add metadata and normalize timestamps
                    timestamp_fields = ["date", "date_start", "date_stop", "end_time"]
                    set_execution_metadata(all_data, exec_id, source=source)
                    for record in all_data:
                        record.update(ensure_string_ids(record))
                        # Parse timestamp fields using centralized utility
                        for field in timestamp_fields:
                            if field in record and record[field] is not None:
                                record[field] = parse_facebook_timestamp(record[field])
                        for key, value in list(record.items()):
                            if isinstance(value, (dict, list)):
                                record[key] = json.dumps(value)

                    # Extract to Parquet
                    base_table_name = table_config.get(
                        "table_name", f"{source}_{table}"
                    )
                    table_name_with_affix = apply_table_affix(
                        base_table_name, schema_prefix, schema_suffix
                    )
                    production_table_id = (
                        f"{env.project}.{main_dataset}.{table_name_with_affix}"
                    )
                    local_file_path = extract_to_local_parquet(
                        data=all_data,
                        source="facebook",
                        table=table,
                        job_id=exec_id,
                        bq_client=bq_client,
                        production_table_id=production_table_id,
                        rebuild_mode=rebuild,
                    )

                    if local_file_path:
                        result.record_prewritten(table, local_file_path, len(all_data))
                        table_success = True
                        logger.info(f"  Parquet file created: {local_file_path}")
                else:
                    logger.info(
                        f"  No page insights extracted (zero rows - normal for incremental run)"
                    )
                    result.skip_table(table, reason="zero_rows")
                    table_success = True

            except Exception as e:
                logger.error(f"  Page insights extraction failed: {e}")
                result.fail_table(table, error=str(e), error_type=type(e).__name__)

        elif table == "post_insights":
            try:
                time.sleep(BATCH_DELAY)
                logger.info(f"Extracting post_insights")
                table_config = config.get("resources", {}).get(table, {})
                # Use spool if populated (posts already extracted); an empty
                # run-level spool must not shadow a populated global one
                post_spool = (
                    spool if spool and spool.size() > 0 else get_current_spool()
                )
                records = list(
                    extract_post_insights(
                        sites,
                        date_range,
                        table_config,
                        pages=_cached_pages,
                        spool=post_spool,
                    )
                )
                logger.info(f"  Found {len(records)} post insight records")
                all_data.extend(records)

                if records:
                    # Add metadata and normalize timestamps
                    timestamp_fields = ["date", "date_start", "date_stop", "end_time"]
                    set_execution_metadata(all_data, exec_id, source=source)
                    for record in all_data:
                        record.update(ensure_string_ids(record))
                        # Parse timestamp fields using centralized utility
                        for field in timestamp_fields:
                            if field in record and record[field] is not None:
                                record[field] = parse_facebook_timestamp(record[field])
                        for key, value in list(record.items()):
                            if isinstance(value, (dict, list)):
                                record[key] = json.dumps(value)

                    # Extract to Parquet
                    base_table_name = table_config.get(
                        "table_name", f"{source}_{table}"
                    )
                    table_name_with_affix = apply_table_affix(
                        base_table_name, schema_prefix, schema_suffix
                    )
                    production_table_id = (
                        f"{env.project}.{main_dataset}.{table_name_with_affix}"
                    )
                    local_file_path = extract_to_local_parquet(
                        data=all_data,
                        source="facebook",
                        table=table,
                        job_id=exec_id,
                        bq_client=bq_client,
                        production_table_id=production_table_id,
                        rebuild_mode=rebuild,
                    )

                    if local_file_path:
                        result.record_prewritten(table, local_file_path, len(all_data))
                        table_success = True
                        logger.info(f"  Parquet file created: {local_file_path}")
                else:
                    logger.info(
                        f"  No post insights extracted (zero rows - normal for incremental run)"
                    )
                    result.skip_table(table, reason="zero_rows")
                    table_success = True

            except Exception as e:
                logger.error(f"  Post insights extraction failed: {e}")
                result.fail_table(table, error=str(e), error_type=type(e).__name__)

        elif table == "post_attachments":
            try:
                time.sleep(BATCH_DELAY)
                logger.info(f"Extracting post_attachments")
                table_config = config.get("resources", {}).get(table, {})
                # Use spool if populated (posts already extracted); an empty
                # run-level spool must not shadow a populated global one
                post_spool = (
                    spool if spool and spool.size() > 0 else get_current_spool()
                )
                records = list(
                    extract_post_attachments(
                        sites,
                        date_range,
                        table_config,
                        pages=_cached_pages,
                        spool=post_spool,
                    )
                )
                logger.info(f"  Found {len(records)} post attachment records")
                all_data.extend(records)

                if records:
                    # Add metadata and normalize timestamps
                    timestamp_fields = ["created_time"]
                    set_execution_metadata(all_data, exec_id, source=source)
                    for record in all_data:
                        record.update(ensure_string_ids(record))
                        # Parse timestamp fields using centralized utility
                        for field in timestamp_fields:
                            if field in record and record[field] is not None:
                                record[field] = parse_facebook_timestamp(record[field])
                        for key, value in list(record.items()):
                            if isinstance(value, (dict, list)):
                                record[key] = json.dumps(value)

                    # Extract to Parquet
                    base_table_name = table_config.get(
                        "table_name", f"{source}_{table}"
                    )
                    table_name_with_affix = apply_table_affix(
                        base_table_name, schema_prefix, schema_suffix
                    )
                    production_table_id = (
                        f"{env.project}.{main_dataset}.{table_name_with_affix}"
                    )
                    local_file_path = extract_to_local_parquet(
                        data=all_data,
                        source="facebook",
                        table=table,
                        job_id=exec_id,
                        bq_client=bq_client,
                        production_table_id=production_table_id,
                        rebuild_mode=rebuild,
                    )

                    if local_file_path:
                        result.record_prewritten(table, local_file_path, len(all_data))
                        table_success = True
                        logger.info(f"  Parquet file created: {local_file_path}")
                else:
                    logger.info(
                        f"  No post attachments extracted (zero rows - normal for incremental run)"
                    )
                    result.skip_table(table, reason="zero_rows")
                    table_success = True

            except Exception as e:
                logger.error(f"  Post attachments extraction failed: {e}")
                result.fail_table(table, error=str(e), error_type=type(e).__name__)

        else:
            logger.warning(f"  Unknown table type: {table}")
            continue

        # ALL TABLES use Parquet -> GCS -> External Tables -> Staging -> Production
        # No more legacy direct BigQuery loads
        parquet_gcs_tables = [
            "pages",
            "posts",
            "campaigns",
            "adsets",
            "ads",
            "adcreatives",
            "adimages",
            "customaudiences",
            "adaccounts",
            "account_insights",
            "campaign_insights",
            "adset_insights",
            "ad_insights",
            "ad_insights_actions",
            "post_insights",
            "post_attachments",
            "page_insights",
            "leads",
            "ad_page_mapping",
        ]
        # Standard pattern: every active Facebook table writes a Parquet file
        # (table_files) and the orchestrator does the GCS upload, external
        # table, staging, and hash merge -- the same path the other extractors
        # use. The legacy in-plugin batch-load + merge that used to live here
        # was already inert (gated on `table not in parquet_gcs_tables`, and
        # every active table is in that list) and has been removed.
        #
        # Safety net: if a table ever reaches here without a Parquet file and
        # isn't in the allowlist, that's a config/extractor mismatch (e.g. a new
        # table added to config but not given an extraction branch). Surface it
        # loudly instead of silently dropping the data the old path would have
        # batch-loaded.
        if table not in parquet_gcs_tables and not table_success:
            logger.error(
                f"  No Parquet file produced for '{table}' and it is not on the "
                f"standard parquet/GCS path. This table has no extraction branch "
                f"-- add one (extract_to_local_parquet) or remove it from config."
            )
            result.fail_table(
                table,
                error=(
                    f"No Parquet file produced for '{table}' and it is not on the "
                    f"standard parquet/GCS path (no extraction branch)."
                ),
            )

        logger.info(f"Table {table} complete")

    # Log post spool statistics if it was used
    if spool:
        spool_stats = spool.stats()
        logger.info(f"\nPost Spool Statistics:")
        logger.info(f"  Posts in spool: {spool_stats['post_count']}")
        logger.info(f"  Cache hits: {spool_stats['cache_hits']}")
        logger.info(f"  Cache misses: {spool_stats['cache_misses']}")
        logger.info(f"  Hit rate: {spool_stats['hit_rate']:.1f}%")
        reset_spool()

    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total rows extracted: {result.total_rows}")
    logger.info(f"Tables processed: {len(tables)}")
    logger.info(f"  Successful: {result.successful_tables}")
    logger.info(f"  Failed: {result.failed_tables}")
    logger.info(f"Accounts processed: {len(sites)}")
    logger.info(f"Rate limit hits: {rate_limit_hits}")

    if rate_limit_hits > 0:
        logger.info(
            "\n TIP: Consider running extractions at different times or reducing batch sizes"
        )

    logger.info("=" * 60)

    # finalize() emits the standard shape (total_rows, table_files keyed by
    # logical name, table_rows, table_status, successful/failed_tables, tables,
    # errors, sites). rate_limit_hits is facebook-specific, carried via **extra.
    return result.finalize(rate_limit_hits=rate_limit_hits)
