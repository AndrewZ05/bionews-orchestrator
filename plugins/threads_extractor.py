#!/usr/bin/env python3
# Threads-specific extraction via Threads API (graph.threads.net/v1.0)
# The facebook_business SDK does NOT support Threads — uses raw HTTP via requests.

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

# Import centralized timestamp utilities (Meta timestamps are same format)
from shared.timestamp_utils import parse_facebook_timestamp
from shared.extractor_utils import apply_table_affix, get_available_tables
from shared.account_context import set_execution_metadata
from shared.extractor_runner import initialize_pipeline_environment

# Threads HTTP client (SDK does not support Threads)
from shared.threads_client import (
    THREADS_API_BASE,
    THREADS_RATE_LIMITS,
    create_threads_session,
    wait_for_threads,
    threads_api_request,
    paginate_threads_api,
    refresh_threads_token,
)

# Import shared utilities
from shared.rate_limiter import (
    with_rate_limit,
    handle_rate_limit_error,
    reset_rate_limit,
)
from shared.bigquery_utils import (
    batch_load_to_bigquery,
    create_table_if_not_exists,
    process_hash_merge,
)
from shared.data_processor import ensure_string_ids
from shared.schema_registry import get_bq_schema
from shared.gcs_pipeline import extract_to_local_parquet

logger = logging.getLogger(__name__)

RATE_LIMIT_DELAY = 1.5
BATCH_DELAY = 0.5


def _try_auto_refresh_token(session, access_token: str, username: str = None) -> str:
    """Attempt to auto-refresh a Threads token if near expiry.

    Checks token store (multi-account) or .threads_token_meta.json (legacy).
    Returns the (possibly new) access token.
    """
    try:
        now = datetime.now(timezone.utc).timestamp()

        # Multi-account: check token store
        token_file = Path(__file__).resolve().parent.parent / ".threads_tokens.json"
        if token_file.exists() and username:
            store = json.loads(token_file.read_text())
            if username in store:
                expires_at = store[username].get("expires_at", 0)
                remaining_days = (expires_at - now) / 86400

                if remaining_days <= 0:
                    logger.warning(
                        f"Token for @{username} EXPIRED. Run: python shared/threads_token_manager.py --add-token"
                    )
                    return access_token
                if remaining_days <= 7:
                    logger.info(
                        f"Token for @{username} expires in {remaining_days:.1f} days. Auto-refreshing..."
                    )
                    new_token = refresh_threads_token(session, access_token)
                    if new_token:
                        from shared.threads_token_manager import save_account_token

                        save_account_token(
                            username, store[username]["user_id"], new_token, 5184000
                        )
                        return new_token
                else:
                    logger.debug(
                        f"Token for @{username} valid for {remaining_days:.0f} more days"
                    )
                return access_token

        # Legacy single-account: check meta file
        meta_file = Path(__file__).resolve().parent.parent / ".threads_token_meta.json"
        if meta_file.exists():
            meta = json.loads(meta_file.read_text())
            expires_at = meta.get("expires_at", 0)
            remaining_days = (expires_at - now) / 86400

            if remaining_days <= 0:
                logger.warning("Threads token EXPIRED.")
                return access_token
            if remaining_days <= 7:
                logger.info(
                    f"Threads token expires in {remaining_days:.1f} days. Auto-refreshing..."
                )
                new_token = refresh_threads_token(session, access_token)
                if new_token:
                    from shared.threads_token_manager import (
                        update_env_file,
                        save_token_metadata,
                    )

                    update_env_file("THREADS_ACCESS_TOKEN", new_token)
                    save_token_metadata(new_token, 5184000)
                    return new_token
            else:
                logger.debug(f"Threads token valid for {remaining_days:.0f} more days")

    except Exception as e:
        logger.debug(f"Auto-refresh check skipped: {e}")

    return access_token


def _load_accounts(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Load Threads accounts from token store, falling back to .env.

    Returns list of dicts: [{user_id, access_token, username}, ...]
    """
    token_file = Path(__file__).resolve().parent.parent / ".threads_tokens.json"

    if token_file.exists():
        try:
            store = json.loads(token_file.read_text())
            if store:
                accounts = []
                for uname, info in store.items():
                    accounts.append(
                        {
                            "user_id": info["user_id"],
                            "access_token": info["access_token"],
                            "username": uname,
                        }
                    )
                logger.info(
                    f"Loaded {len(accounts)} Threads account(s) from token store"
                )
                return accounts
        except (json.JSONDecodeError, IOError, KeyError) as e:
            logger.warning(f"Error loading token store: {e}")

    # Fallback: single account from env/config
    access_token = config.get("source", {}).get("connection", {}).get(
        "access_token"
    ) or os.getenv("THREADS_ACCESS_TOKEN")
    user_id = os.getenv("THREADS_USER_ID")

    if access_token and user_id:
        logger.info("Using single Threads account from .env")
        return [
            {"user_id": user_id, "access_token": access_token, "username": "default"}
        ]

    return []


def initialize_session(config: Dict[str, Any]) -> Tuple[Any, str, str]:
    """Initialize Threads API session and resolve user ID.

    Returns (session, access_token, user_id) or raises Exception.
    Used for single-account legacy mode.
    """
    access_token = config.get("source", {}).get("connection", {}).get(
        "access_token"
    ) or os.getenv("THREADS_ACCESS_TOKEN")

    if not access_token:
        raise Exception(
            "Threads access token not found. Set THREADS_ACCESS_TOKEN environment variable."
        )

    session = create_threads_session()
    access_token = _try_auto_refresh_token(session, access_token)

    try:
        profile = threads_api_request(
            session, "me", access_token, params={"fields": "id,username"}
        )
        user_id = profile.get("id", "")
        username = profile.get("username", "")
        logger.info(f"Threads API initialized: @{username} (ID: {user_id})")
        return session, access_token, user_id
    except Exception as e:
        raise Exception(f"Failed to initialize Threads API: {e}")


def get_available_sites(config: Dict[str, Any]) -> List[str]:
    """Get available Threads user IDs from token store or env."""
    # Multi-account: return user IDs from token store
    token_file = Path(__file__).resolve().parent.parent / ".threads_tokens.json"
    if token_file.exists():
        try:
            store = json.loads(token_file.read_text())
            if store:
                return [info["user_id"] for info in store.values()]
        except (json.JSONDecodeError, IOError, KeyError):
            pass

    user_id = os.getenv("THREADS_USER_ID")
    if user_id:
        return (
            [uid.strip() for uid in user_id.split(",")] if "," in user_id else [user_id]
        )
    return []


def extract_profiles(
    session, access_token: str, user_ids: List[str]
) -> Iterator[Dict[str, Any]]:
    """Extract Threads user profiles.

    Endpoint: GET /v1.0/me?fields=id,username,name,threads_profile_picture_url,threads_biography
    """
    fields = "id,username,name,threads_profile_picture_url,threads_biography"

    for user_id in user_ids:
        try:
            wait_for_threads("default")

            # Use 'me' for the authenticated user or the specific user_id
            endpoint = "me" if user_id == "me" else user_id
            profile = threads_api_request(
                session,
                endpoint,
                access_token,
                params={
                    "fields": fields,
                },
            )

            record = {
                "threads_user_id": str(profile.get("id", user_id)),
                "username": profile.get("username", ""),
                "name": profile.get("name", ""),
                "threads_profile_picture_url": profile.get(
                    "threads_profile_picture_url", ""
                ),
                "threads_biography": profile.get("threads_biography", ""),
            }
            yield record
            logger.info(f"  Extracted profile for @{record['username']}")

        except Exception as e:
            logger.error(f"  Profile extraction failed for user {user_id}: {e}")


def extract_posts(
    session,
    access_token: str,
    user_ids: List[str],
    date_range: Tuple[date, date],
    test_mode: bool = False,
) -> Iterator[Dict[str, Any]]:
    """Extract Threads posts with date filtering and pagination.

    Endpoint: GET /v1.0/{user_id}/threads?fields=...&since=X&until=Y
    Supports since/until as Unix timestamps or ISO 8601.
    """
    start_date, end_date = date_range
    fields = (
        "id,media_product_type,media_type,media_url,permalink,text,"
        "timestamp,shortcode,thumbnail_url,is_quote_post,alt_text,"
        "link_attachment_url,has_replies,is_reply"
    )

    for user_id in user_ids:
        logger.info(f"  Extracting posts for user {user_id}")

        try:
            params = {
                "fields": fields,
                "limit": 100,
                "since": int(
                    datetime.combine(start_date, datetime.min.time())
                    .replace(tzinfo=timezone.utc)
                    .timestamp()
                ),
                "until": int(
                    datetime.combine(end_date + timedelta(days=1), datetime.min.time())
                    .replace(tzinfo=timezone.utc)
                    .timestamp()
                ),
            }

            count = 0
            test_limit = 25 if test_mode else None

            for post in paginate_threads_api(
                session,
                f"{user_id}/threads",
                access_token,
                params=params,
                endpoint_type="posts",
                limit=test_limit,
            ):
                record = {
                    "id": str(post.get("id", "")),
                    "threads_user_id": user_id,
                    "media_product_type": post.get("media_product_type", ""),
                    "media_type": post.get("media_type", ""),
                    "media_url": post.get("media_url", ""),
                    "permalink": post.get("permalink", ""),
                    "text": post.get("text", ""),
                    "timestamp": post.get("timestamp", ""),
                    "shortcode": post.get("shortcode", ""),
                    "thumbnail_url": post.get("thumbnail_url", ""),
                    "is_quote_post": str(post.get("is_quote_post", "")),
                    "alt_text": post.get("alt_text", ""),
                    "link_attachment_url": post.get("link_attachment_url", ""),
                    "has_replies": str(post.get("has_replies", "")),
                    "is_reply": str(post.get("is_reply", "")),
                }
                yield record
                count += 1

            logger.info(f"    Extracted {count} posts for user {user_id}")

        except Exception as e:
            logger.error(f"    Posts extraction failed for user {user_id}: {e}")


def extract_post_insights(
    session,
    access_token: str,
    post_records: List[Dict[str, Any]],
    config: Dict[str, Any],
) -> Iterator[Dict[str, Any]]:
    """Extract per-post engagement insights (lifetime totals).

    Endpoint: GET /v1.0/{media_id}/insights?metric=views,likes,replies,reposts,quotes,shares
    """
    table_config = config.get("resources", {}).get("post_insights", {})
    metrics = table_config.get("params", {}).get(
        "metric", ["views", "likes", "replies", "reposts", "quotes", "shares"]
    )

    for post_rec in post_records:
        post_id = post_rec.get("id")
        threads_user_id = post_rec.get("threads_user_id", "")

        if not post_id:
            continue

        # Skip reposts — they don't support insights
        if post_rec.get("media_type") == "REPOST_FACADE":
            continue

        try:
            wait_for_threads("insights")

            data = threads_api_request(
                session,
                f"{post_id}/insights",
                access_token,
                params={
                    "metric": ",".join(metrics),
                },
                endpoint_type="insights",
            )

            record = {
                "thread_id": str(post_id),
                "threads_user_id": threads_user_id,
                "timestamp": post_rec.get("timestamp", ""),
            }

            # Flatten insights: [{name: 'views', values: [{value: 123}]}, ...]
            for insight in data.get("data", []):
                metric_name = insight.get("name", "")
                values = insight.get("values", [])
                if values and isinstance(values, list) and len(values) > 0:
                    record[metric_name] = str(values[0].get("value", 0))
                else:
                    record[metric_name] = "0"

            yield record

        except Exception as e:
            logger.debug(f"    Insights not available for post {post_id}: {e}")


def extract_user_insights(
    session,
    access_token: str,
    user_ids: List[str],
    date_range: Tuple[date, date],
    config: Dict[str, Any],
    profiles: Optional[List[Dict[str, Any]]] = None,
) -> Iterator[Dict[str, Any]]:
    """Extract user-level daily insights with date chunking.

    Endpoint: GET /v1.0/{user_id}/threads_insights?metric=...&since=X&until=Y&period=day
    """
    start_date, end_date = date_range
    table_config = config.get("resources", {}).get("user_insights", {})
    metrics = table_config.get("params", {}).get(
        "metric", ["views", "likes", "replies", "reposts", "quotes", "followers_count"]
    )

    CHUNK_DAYS = 90

    # Build username lookup
    username_lookup = {}
    if profiles:
        for p in profiles:
            username_lookup[p.get("threads_user_id", "")] = p.get("username", "")

    for user_id in user_ids:
        username = username_lookup.get(user_id, "")
        logger.info(f"  Extracting user insights for {user_id}")

        current_start = start_date
        total_records = 0

        while current_start <= end_date:
            chunk_end = min(current_start + timedelta(days=CHUNK_DAYS - 1), end_date)

            try:
                wait_for_threads("insights")

                data = threads_api_request(
                    session,
                    f"{user_id}/threads_insights",
                    access_token,
                    params={
                        "metric": ",".join(metrics),
                        "since": int(
                            datetime.combine(current_start, datetime.min.time())
                            .replace(tzinfo=timezone.utc)
                            .timestamp()
                        ),
                        "until": int(
                            datetime.combine(
                                chunk_end + timedelta(days=1), datetime.min.time()
                            )
                            .replace(tzinfo=timezone.utc)
                            .timestamp()
                        ),
                        "period": "day",
                    },
                    endpoint_type="insights",
                )

                # Pivot from metric-per-day to one-row-per-day
                daily_data = {}

                for insight in data.get("data", []):
                    metric_name = insight.get("name", "")
                    values = insight.get("values", [])

                    for val_entry in values:
                        end_time = val_entry.get("end_time", "")
                        value = val_entry.get("value", 0)

                        if end_time not in daily_data:
                            daily_data[end_time] = {}
                        daily_data[end_time][metric_name] = str(value)

                for date_str, metric_values in daily_data.items():
                    record = {
                        "threads_user_id": user_id,
                        "username": username,
                        "date": date_str,
                    }
                    for m in metrics:
                        record[m] = metric_values.get(m, "0")

                    yield record
                    total_records += 1

            except Exception as e:
                logger.warning(
                    f"    User insights chunk {current_start} to {chunk_end} failed: {e}"
                )

            current_start = chunk_end + timedelta(days=1)

        logger.info(
            f"    Extracted {total_records} daily insight records for user {user_id}"
        )


def _process_records_to_parquet(
    records,
    table,
    config,
    env,
    main_dataset,
    exec_id,
    source,
    bq_client,
    schema_prefix,
    schema_suffix,
    rebuild,
    timestamp_fields=None,
):
    """Common helper to add metadata, normalize timestamps, and write records to Parquet."""
    if not records:
        return None, 0

    if timestamp_fields is None:
        timestamp_fields = ["timestamp", "date"]

    set_execution_metadata(records, exec_id, source=source)

    for record in records:
        for field in timestamp_fields:
            if field in record and record[field] is not None:
                record[field] = parse_facebook_timestamp(record[field])

        record.update(ensure_string_ids(record))

        for key, value in list(record.items()):
            if isinstance(value, (dict, list)):
                record[key] = json.dumps(value)

    table_config = config.get("resources", {}).get(table, {})
    base_table_name = table_config.get("table_name", f"{source}_{table}")
    table_name_with_affix = apply_table_affix(
        base_table_name, schema_prefix, schema_suffix
    )
    production_table_id = f"{env.project}.{main_dataset}.{table_name_with_affix}"

    local_file_path = extract_to_local_parquet(
        data=records,
        source="threads",
        table=table,
        job_id=exec_id,
        bq_client=bq_client,
        production_table_id=production_table_id,
        rebuild_mode=rebuild,
    )

    return local_file_path, len(records)


def run_pipeline(
    config: Dict[str, Any],
    sites: List[str],
    tables: List[str],
    group: Optional[str],
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    end_date: Optional[str],
    test_mode: bool,
    batch_size: Optional[int],
    max_retries: int,
    skip_validation: bool,
    export_format: Optional[str],
    export_dir: str,
    skip_hash_merge: bool,
    archive_staging: bool,
    truncate_staging: bool,
    rebuild: bool,
    bq_client: Any,
    execution_id: str,
    parallel_workers: Optional[int] = None,
    schema_prefix: Optional[str] = None,
    schema_suffix: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:

    from dotenv import load_dotenv

    load_dotenv()

    # Load accounts from multi-account token store (or fall back to .env)
    accounts = _load_accounts(config)

    if not accounts:
        raise Exception(
            "No Threads accounts configured. Run:\n"
            "  python shared/threads_token_manager.py --add-token TOKEN\n"
            "to add accounts from the User Token Generator."
        )

    # Get tables if not specified
    if not tables:
        tables = get_available_tables(config, group)
        if not tables:
            logger.error("No tables specified or found in group")
            return {"total_rows": 0, "sites": 0, "tables": 0}

    logger.info(
        f"Threads pipeline: extracting {len(tables)} tables for {len(accounts)} account(s)"
    )
    for acct in accounts:
        logger.info(f"  @{acct['username']} (ID: {acct['user_id']})")

    # Parse date range
    if start_date and end_date:
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
        end_dt = date.today()
        max_months = (
            config.get("extractor", {}).get("full_refresh", {}).get("max_months", 12)
        )
        start_dt = end_dt - timedelta(days=max_months * 30)
        date_range = (start_dt, end_dt)
        logger.info(f"FULL REFRESH MODE: Extracting {max_months} months of data")
    else:
        end_dt = date.today()
        if lookback_days:
            days_back = lookback_days
        else:
            days_back = config.get("extractor", {}).get("lookback_days", 30)
        start_dt = end_dt - timedelta(days=days_back)
        date_range = (start_dt, end_dt)

    logger.info(f"Date range: {date_range[0]} to {date_range[1]}")

    # Initialize shared pipeline environment
    env = initialize_pipeline_environment(
        config,
        bq_client=bq_client,
        source_default="threads",
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
    exec_id = env.execution_id or execution_id

    # Standardized per-table accounting + return shape (shared/extraction_result).
    # NOTE: Threads is NOT live as of this migration, so this could NOT be
    # live-parity-verified the way instagram/mailchimp were. It relies on unit
    # tests + the instagram precedent (same Facebook-family shape). Run a
    # supervised parity check (flag-off vs the standard path) before trusting it
    # when Threads is reactivated.
    from shared.extraction_result import StandardExtractionResult

    result = StandardExtractionResult(
        source=source,
        execution_id=exec_id,
        job_id=exec_id,
        bq_client=bq_client,
        sites=accounts,
    )

    # Accumulate records per table across all accounts
    table_records = {table: [] for table in tables}

    for account in accounts:
        username = account["username"]
        user_id = account["user_id"]

        logger.info(f"\n{'='*40}")
        logger.info(f"Extracting @{username} (ID: {user_id})")
        logger.info(f"{'='*40}")

        session = create_threads_session()
        access_token = account["access_token"]

        # Auto-refresh if near expiry
        access_token = _try_auto_refresh_token(session, access_token, username)

        # Verify token works
        try:
            profile = threads_api_request(
                session, "me", access_token, params={"fields": "id,username"}
            )
            logger.info(f"  Token verified: @{profile.get('username')}")
        except Exception as e:
            logger.error(f"  Token for @{username} is invalid: {e}. Skipping account.")
            continue

        acct_post_cache = []
        acct_profile_cache = []

        for table in tables:
            table_config = config.get("resources", {}).get(table, {})
            if not table_config.get("active", True):
                continue

            try:
                if table == "profiles":
                    recs = list(extract_profiles(session, access_token, [user_id]))
                    acct_profile_cache = recs
                    table_records[table].extend(recs)
                    logger.info(f"  profiles: {len(recs)} record(s)")

                elif table == "posts":
                    recs = list(
                        extract_posts(
                            session, access_token, [user_id], date_range, test_mode
                        )
                    )
                    acct_post_cache = recs
                    table_records[table].extend(recs)
                    logger.info(f"  posts: {len(recs)} record(s)")

                elif table == "post_insights":
                    if not acct_post_cache:
                        acct_post_cache = list(
                            extract_posts(
                                session, access_token, [user_id], date_range, test_mode
                            )
                        )
                        table_records.setdefault("posts", []).extend(acct_post_cache)
                    recs = list(
                        extract_post_insights(
                            session, access_token, acct_post_cache, config
                        )
                    )
                    table_records[table].extend(recs)
                    logger.info(f"  post_insights: {len(recs)} record(s)")

                elif table == "user_insights":
                    recs = list(
                        extract_user_insights(
                            session,
                            access_token,
                            [user_id],
                            date_range,
                            config,
                            acct_profile_cache,
                        )
                    )
                    table_records[table].extend(recs)
                    logger.info(f"  user_insights: {len(recs)} record(s)")

                else:
                    logger.warning(f"  Unknown table: {table}")

            except Exception as e:
                logger.error(f"  {table} extraction failed for @{username}: {e}")

    # Write accumulated records to Parquet (one file per table)
    logger.info(f"\n{'='*60}")
    logger.info("Writing Parquet files...")
    logger.info(f"{'='*60}")

    for table in tables:
        records = table_records.get(table, [])
        if not records:
            logger.info(f"  {table}: no data (recorded as zero_rows)")
            result.skip_table(table, reason="zero_rows")
            continue

        logger.info(
            f"  {table}: {len(records)} total records from {len(accounts)} account(s)"
        )

        try:
            ts_fields = (
                []
                if table == "profiles"
                else (["date"] if table == "user_insights" else None)
            )
            local_file, row_count = _process_records_to_parquet(
                records,
                table,
                config,
                env,
                main_dataset,
                exec_id,
                source,
                bq_client,
                schema_prefix,
                schema_suffix,
                rebuild,
                timestamp_fields=ts_fields,
            )

            # Plugin wrote the Parquet (_process_records_to_parquet); record the
            # existing path + count. Falsy local_file -> zero rows (valid steady
            # state), recorded as zero_rows not failed.
            if local_file:
                logger.info(f"    Parquet: {local_file} ({row_count} rows)")
            result.record_prewritten(table, local_file, row_count or 0)

        except Exception as e:
            logger.error(f"    {table} parquet write failed: {e}")
            result.fail_table(
                table, error=str(e), stage="transform", error_type=type(e).__name__
            )

    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("THREADS PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total rows extracted: {result.total_rows}")
    logger.info(f"Accounts processed: {len(accounts)}")
    logger.info(
        f"Tables: {result.successful_tables} successful, {result.failed_tables} failed"
    )
    logger.info("=" * 60)

    return result.finalize()
