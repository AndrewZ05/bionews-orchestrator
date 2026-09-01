#!/usr/bin/env python3
# Instagram-specific extraction via Instagram Graph API (facebook_business SDK v25.0)

import os
import sys
import time
import json
import concurrent.futures
import logging
from typing import Dict, List, Any, Iterator, Optional, Tuple
from datetime import datetime, timedelta, date, timezone
from pathlib import Path
import pandas as pd
from google.cloud import bigquery

# Import centralized timestamp utilities
from shared.timestamp_utils import parse_facebook_timestamp
from shared.extractor_utils import apply_table_affix, get_available_tables
from shared.account_context import set_execution_metadata
from shared.extractor_runner import initialize_pipeline_environment

# Instagram uses the same facebook_business SDK (graph.facebook.com/v25.0)
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.page import Page
from facebook_business.adobjects.user import User
from facebook_business.adobjects.iguser import IGUser
from facebook_business.adobjects.igmedia import IGMedia
from facebook_business.exceptions import FacebookRequestError

# Import shared utilities
from shared.rate_limiter import (
    with_rate_limit,
    handle_rate_limit_error,
    reset_rate_limit,
    wait_if_needed,
    create_rate_limiter,
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

# Rate limiting via shared token-bucket rate limiter
# Instagram Graph API limit: 200 calls/user/hour (2025+). At 200/hr the steady-state
# rate is 0.0556 calls/sec. We refill at that rate so a multi-hour backfill can
# sustain the budget instead of burning the whole hour in 100 seconds and then
# spending the rest in reactive 429 backoff.
_IG_LIMITER = "instagram_api"
_IG_BURST_RATE = 200.0 / 3600.0  # ~0.0556 calls/sec = 200/hour, matches API quota
_access_token_cache = None  # Cached for potential batch API use


def _rate_limit():
    """Wait if needed before making an actual Instagram API call."""
    wait_if_needed(_IG_LIMITER)


def _on_api_error(error):
    """Handle API error with rate-limit-aware exponential backoff."""
    handle_rate_limit_error(_IG_LIMITER, error)


def initialize_api(config: Dict[str, Any]) -> bool:
    """Initialize Facebook API (shared with Instagram Graph API)."""
    global _access_token_cache
    try:
        access_token = config.get("source", {}).get("connection", {}).get(
            "access_token"
        ) or os.getenv("FACEBOOK_ACCESS_TOKEN")
        app_id = os.getenv("FACEBOOK_APP_ID")
        app_secret = os.getenv("FACEBOOK_APP_SECRET")

        if not access_token:
            logger.error(
                "Facebook access token not found (required for Instagram Graph API)"
            )
            return False

        FacebookAdsApi.init(app_id, app_secret, access_token, api_version="v25.0")
        _access_token_cache = access_token
        # 1-hour max backoff: matches the API's hourly quota window so a 429 storm
        # waits for quota refill instead of busy-looping at the 5-minute default cap.
        create_rate_limiter(
            calls_per_second=_IG_BURST_RATE,
            name=_IG_LIMITER,
            max_backoff_seconds=3600,
        )
        logger.info(
            "Instagram API initialized successfully (via Facebook Graph API v25.0)"
        )
        return True

    except Exception as e:
        logger.error(f"Failed to initialize Instagram API: {e}")
        return False


def get_available_sites(config: Dict[str, Any]) -> List[str]:
    """Get available Facebook account IDs (used for IG account discovery via Pages)."""
    if not initialize_api(config):
        return []

    account_id = os.getenv("FACEBOOK_ACCOUNT_ID")
    if account_id:
        sites = (
            [aid.strip() for aid in account_id.split(",")]
            if "," in account_id
            else [account_id]
        )
        return sites
    return []


def discover_ig_accounts(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Discover Instagram Business accounts linked to Facebook Pages.

    Uses nested field expansion to fetch IG account details in a single API call
    instead of one call per page (eliminates N+1 query pattern).
    Flow: GET /me/accounts?fields=...,instagram_business_account{id,username,...}
    """
    ig_accounts = []

    try:
        # Get all Facebook Pages with IG account details in one call via nested fields
        _rate_limit()
        user = User("me")
        pages = user.get_accounts(
            fields=[
                "id",
                "name",
                "access_token",
                "instagram_business_account{id,username,name,biography,website,"
                "profile_picture_url,followers_count,follows_count,media_count}",
            ]
        )

        for page_data in pages:
            ig_biz = page_data.get("instagram_business_account")
            if not ig_biz:
                logger.debug(
                    f"Page '{page_data.get('name')}' has no Instagram Business account linked"
                )
                continue

            ig_user_id = ig_biz.get("id")
            if not ig_user_id:
                continue

            # All IG account fields already available from nested expansion
            account = {
                "ig_user_id": str(ig_biz.get("id", ig_user_id)),
                "username": ig_biz.get("username", ""),
                "name": ig_biz.get("name", ""),
                "biography": ig_biz.get("biography", ""),
                "website": ig_biz.get("website", ""),
                "profile_picture_url": ig_biz.get("profile_picture_url", ""),
                "followers_count": ig_biz.get("followers_count", 0),
                "follows_count": ig_biz.get("follows_count", 0),
                "media_count": ig_biz.get("media_count", 0),
                "page_id": str(page_data.get("id", "")),
                "page_name": page_data.get("name", ""),
                "_page_access_token": page_data.get("access_token", ""),
            }

            ig_accounts.append(account)
            logger.info(
                f"  Discovered IG account: @{account['username']} (ID: {ig_user_id}) via page '{account['page_name']}'"
            )

    except FacebookRequestError as e:
        logger.error(f"Failed to discover IG accounts: {e}")
    except Exception as e:
        logger.error(f"Unexpected error discovering IG accounts: {e}")

    logger.info(f"Discovered {len(ig_accounts)} Instagram Business accounts")
    return ig_accounts


def extract_ig_accounts(
    ig_accounts: List[Dict[str, Any]], account_id: str
) -> Iterator[Dict[str, Any]]:
    """Yield IG account records (already fetched during discovery)."""
    for acct in ig_accounts:
        record = {k: v for k, v in acct.items() if not k.startswith("_")}
        record["account_id"] = account_id
        yield record


def _is_rate_limit_error(e: Exception) -> bool:
    """Detect Instagram/Facebook rate-limit errors by code or message."""
    # facebook_business FacebookRequestError exposes api_error_code()/api_error_subcode()
    if hasattr(e, "api_error_code"):
        try:
            code = e.api_error_code()
            # 4 = App-level rate limit, 17 = User-level rate limit,
            # 32 = Page-level rate limit, 613 = Custom-level rate limit (4-hr window)
            if code in (4, 17, 32, 613):
                return True
        except Exception:
            pass
    msg = str(e).lower()
    return any(
        t in msg
        for t in ("rate limit", "too many requests", "#4)", "#17)", "#32)", "#613)")
    )


def _extract_media_for_account(
    acct: Dict[str, Any],
    date_range: Tuple[date, date],
    account_id: str,
    fields: List[str],
    test_mode: bool = False,
    max_rate_limit_retries: int = 3,
) -> List[Dict[str, Any]]:
    """Extract media items for a single Instagram account. Returns a list.

    Rate-limits the initial get_media() call but NOT each cursor item,
    since the SDK cursor fetches ~25 items per page internally.
    On rate-limit errors, registers backoff with the limiter and retries
    the account from scratch (up to max_rate_limit_retries times). Partial
    records from the failed attempt are discarded to avoid duplicate-page
    risk after retry.
    """
    start_date, end_date = date_range
    ig_user_id = acct["ig_user_id"]
    logger.info(f"  Extracting media for @{acct['username']} (IG ID: {ig_user_id})")

    for attempt in range(max_rate_limit_retries + 1):
        records = []
        try:
            ig_user = IGUser(ig_user_id)
            _rate_limit()
            media_cursor = ig_user.get_media(fields=fields)

            count = 0
            for media_obj in media_cursor:
                # Rate limit at page boundaries (~25 items/page) to throttle pagination
                if count > 0 and count % 25 == 0:
                    _rate_limit()

                media_dict = dict(media_obj)
                media_ts = media_dict.get("timestamp")

                # Apply date filtering (SDK doesn't support date params for media edge)
                if start_date != date.min and media_ts:
                    parsed_ts = parse_facebook_timestamp(media_ts)
                    if parsed_ts:
                        media_date = (
                            parsed_ts.date()
                            if isinstance(parsed_ts, datetime)
                            else parsed_ts
                        )
                        if media_date < start_date:
                            logger.debug(
                                f"    Reached media before {start_date}, stopping pagination"
                            )
                            break

                record = {
                    "id": str(media_dict.get("id", "")),
                    "ig_user_id": ig_user_id,
                    "caption": media_dict.get("caption", ""),
                    "media_type": media_dict.get("media_type", ""),
                    "media_product_type": media_dict.get("media_product_type", ""),
                    "media_url": media_dict.get("media_url", ""),
                    "thumbnail_url": media_dict.get("thumbnail_url", ""),
                    "permalink": media_dict.get("permalink", ""),
                    "timestamp": media_ts,
                    "like_count": media_dict.get("like_count", 0),
                    "comments_count": media_dict.get("comments_count", 0),
                    "is_shared_to_feed": str(media_dict.get("is_shared_to_feed", "")),
                    "shortcode": media_dict.get("shortcode", ""),
                    "username": media_dict.get("username", acct.get("username", "")),
                    "page_id": acct.get("page_id", ""),
                    "account_id": account_id,
                }
                records.append(record)
                count += 1

                if test_mode and count >= 25:
                    logger.info(f"    Test mode: limiting to {count} media items")
                    break

            logger.info(f"    Extracted {count} media items for @{acct['username']}")
            return records

        except FacebookRequestError as e:
            if _is_rate_limit_error(e) and attempt < max_rate_limit_retries:
                _on_api_error(
                    e
                )  # registers backoff; next _rate_limit() will wait it out
                logger.warning(
                    f"    Rate limited for @{acct['username']} after {len(records)} records "
                    f"(attempt {attempt + 1}/{max_rate_limit_retries + 1}); "
                    f"discarding partial batch and retrying after backoff"
                )
                continue
            _on_api_error(e)
            logger.error(f"    Media extraction failed for @{acct['username']}: {e}")
            return records
        except Exception as e:
            logger.error(
                f"    Unexpected error extracting media for @{acct['username']}: {e}"
            )
            return records

    logger.error(
        f"    Gave up on @{acct['username']} after {max_rate_limit_retries} rate-limit retries"
    )
    return []


def extract_media(
    ig_accounts: List[Dict[str, Any]],
    date_range: Tuple[date, date],
    account_id: str,
    test_mode: bool = False,
    parallel_workers: int = 4,
) -> Iterator[Dict[str, Any]]:
    """Extract Instagram media from all IG accounts with parallel processing.

    Uses ThreadPoolExecutor to extract from multiple accounts concurrently.
    The shared token-bucket rate limiter coordinates API calls across threads.
    """
    fields = [
        "id",
        "caption",
        "media_type",
        "media_product_type",
        "media_url",
        "thumbnail_url",
        "permalink",
        "timestamp",
        "like_count",
        "comments_count",
        "is_shared_to_feed",
        "shortcode",
        "username",
    ]

    all_records = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=parallel_workers
    ) as executor:
        futures = {
            executor.submit(
                _extract_media_for_account,
                acct,
                date_range,
                account_id,
                fields,
                test_mode,
            ): acct
            for acct in ig_accounts
        }
        for future in concurrent.futures.as_completed(futures):
            acct = futures[future]
            try:
                records = future.result()
                all_records.extend(records)
            except Exception as e:
                logger.error(
                    f"    Failed to extract media for @{acct.get('username', '?')}: {e}"
                )

    for record in all_records:
        yield record


def _extract_insight_for_media(
    media_rec: Dict[str, Any],
    metric_feed: List[str],
    metric_carousel: List[str],
    account_id: str,
) -> Optional[Dict[str, Any]]:
    """Extract insight for a single media item. Returns record or None."""
    media_id = media_rec.get("id")
    if not media_id:
        return None

    media_type = media_rec.get("media_type", "")
    metrics = metric_carousel if media_type == "CAROUSEL_ALBUM" else metric_feed

    try:
        _rate_limit()
        ig_media = IGMedia(media_id)
        insights = ig_media.get_insights(params={"metric": ",".join(metrics)})

        record = {
            "media_id": str(media_id),
            "ig_user_id": media_rec.get("ig_user_id", ""),
            "media_type": media_type,
            "media_product_type": media_rec.get("media_product_type", ""),
            "timestamp": media_rec.get("timestamp", ""),
            "page_id": media_rec.get("page_id", ""),
            "account_id": account_id,
        }

        for insight in insights:
            insight_dict = dict(insight)
            metric_name = insight_dict.get("name", "")
            values = insight_dict.get("values", [])
            if values and isinstance(values, list) and len(values) > 0:
                record[metric_name] = str(values[0].get("value", 0))
            else:
                record[metric_name] = "0"

        # Ensure all metric columns exist (CAROUSEL_ALBUM doesn't support likes/comments/shares)
        for m in metric_feed:
            if m not in record:
                record[m] = "0"

        reset_rate_limit(_IG_LIMITER)
        return record

    except FacebookRequestError as e:
        error_code = (
            getattr(e, "api_error_code", lambda: None)()
            if hasattr(e, "api_error_code")
            else None
        )
        if error_code == 100:
            logger.debug(
                f"    Insights not available for media {media_id} (type={media_type}): {e}"
            )
        elif error_code in (4, 17):  # Rate limit errors
            _on_api_error(e)
            logger.warning(f"    Rate limited on insights for media {media_id}: {e}")
        else:
            logger.warning(f"    Insights extraction failed for media {media_id}: {e}")
    except Exception as e:
        logger.warning(
            f"    Unexpected error getting insights for media {media_id}: {e}"
        )
    return None


def extract_media_insights(
    ig_accounts: List[Dict[str, Any]],
    media_records: List[Dict[str, Any]],
    config: Dict[str, Any],
    account_id: str,
    parallel_workers: int = 4,
) -> Iterator[Dict[str, Any]]:
    """Extract per-media insights with parallel processing.

    FEED/REELS: views, reach, saved, likes, comments, shares, total_interactions
    CAROUSEL_ALBUM: views, reach, saved, total_interactions

    Uses ThreadPoolExecutor since each media item requires its own API call.
    """
    table_config = config.get("resources", {}).get("media_insights", {})
    params = table_config.get("params", {})
    metric_feed = params.get(
        "metric_feed",
        [
            "views",
            "reach",
            "saved",
            "likes",
            "comments",
            "shares",
            "total_interactions",
        ],
    )
    metric_carousel = params.get(
        "metric_carousel", ["views", "reach", "saved", "total_interactions"]
    )

    total = len(media_records)
    completed = 0

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=parallel_workers
    ) as executor:
        futures = {
            executor.submit(
                _extract_insight_for_media,
                media_rec,
                metric_feed,
                metric_carousel,
                account_id,
            ): media_rec
            for media_rec in media_records
        }
        for future in concurrent.futures.as_completed(futures):
            completed += 1
            if completed % 100 == 0:
                logger.info(f"    Media insights progress: {completed}/{total}")
            try:
                record = future.result()
                if record:
                    yield record
            except Exception as e:
                media_rec = futures[future]
                logger.warning(
                    f"    Insight extraction failed for media {media_rec.get('id', '?')}: {e}"
                )


def _fetch_followers_count_snapshot(ig_user_id: str) -> Optional[int]:
    """Fetch the real-time absolute followers_count from the IG User object.

    The insights API follower_count metric with period=day returns the daily NET
    CHANGE (new followers minus unfollows), NOT the total. This function fetches
    the actual total via GET /{ig_user_id}?fields=followers_count.
    """
    try:
        _rate_limit()
        ig_user = IGUser(ig_user_id)
        user_data = ig_user.api_get(fields=["followers_count"])
        return user_data.get("followers_count")
    except Exception as e:
        logger.warning(
            f"    Failed to fetch followers_count snapshot for {ig_user_id}: {e}"
        )
        return None


def extract_account_insights(
    ig_accounts: List[Dict[str, Any]],
    date_range: Tuple[date, date],
    config: Dict[str, Any],
    account_id: str,
) -> Iterator[Dict[str, Any]]:
    """Extract account-level daily insights with 30-day date chunking.

    Available metrics via API (v25.0): reach, follower_count
    NOTE: The insights API follower_count (period=day) returns the daily NET
    CHANGE in followers, NOT the absolute total. The absolute followers_count
    is fetched separately via GET /{ig_user_id}?fields=followers_count and
    stamped onto the most recent day's record.
    Views are derived from media-level aggregation in post-process SQL
    (instagram_account_insights_daily.sql) — the API only supports
    metric_type=total_value for views, requiring 1 call/day/account.
    DEPRECATED (removed v21+): impressions, profile_views, email_contacts, phone_call_clicks, etc.
    """
    start_date, end_date = date_range
    table_config = config.get("resources", {}).get("account_insights", {})
    metrics = table_config.get("params", {}).get("metric", ["reach", "follower_count"])
    period = table_config.get("params", {}).get("period", "day")

    # views is derived from media-level aggregation in post-process SQL.
    # Only extract reach and follower_count via the account insights API.
    metrics_to_extract = [m for m in metrics if m != "views"]

    # Instagram API STRICT LIMIT: Maximum 2,592,000 seconds (exactly 30 days) between since and until
    MAX_SECONDS = 2592000  # 30 days * 86400 seconds/day

    # Instagram API only provides insights for the last 2 years
    two_years_ago = end_date - timedelta(days=730)
    if start_date == date.min or start_date < two_years_ago:
        start_date = two_years_ago

    for acct in ig_accounts:
        ig_user_id = acct["ig_user_id"]
        username = acct.get("username", "")
        logger.info(f"  Extracting account insights for @{username}")

        # Fetch the ABSOLUTE followers_count snapshot from the IG User object.
        # This is the real total (e.g. 9,388) — not the daily delta from insights API.
        followers_count_snapshot = _fetch_followers_count_snapshot(ig_user_id)
        if followers_count_snapshot is not None:
            logger.info(
                f"    Absolute followers_count snapshot for @{username}: {followers_count_snapshot}"
            )
        else:
            # Fall back to the discovery-time value if the live fetch fails
            followers_count_snapshot = acct.get("followers_count", 0)
            logger.warning(
                f"    Using discovery-time followers_count for @{username}: {followers_count_snapshot}"
            )

        ig_user = IGUser(ig_user_id)

        # Chunk date range with STRICT enforcement of 30-day limit
        current_start = start_date
        total_records = 0
        account_records = []  # Collect records to stamp snapshot on most recent day

        while current_start <= end_date:
            # Calculate since_ts (start of current_start day, inclusive)
            since_ts = int(
                datetime.combine(current_start, datetime.min.time())
                .replace(tzinfo=timezone.utc)
                .timestamp()
            )

            # Calculate maximum chunk end date (30 days from current_start: days 0-29 inclusive)
            max_chunk_end = current_start + timedelta(days=29)

            # Actual chunk end is minimum of max_chunk_end and end_date
            chunk_end_dt = min(max_chunk_end, end_date)

            # Calculate until_ts (start of day AFTER chunk_end_dt, exclusive)
            until_ts = int(
                datetime.combine(chunk_end_dt + timedelta(days=1), datetime.min.time())
                .replace(tzinfo=timezone.utc)
                .timestamp()
            )

            # Verify the range is within the 30-day limit (safety check)
            seconds_diff = until_ts - since_ts
            if seconds_diff > MAX_SECONDS:
                logger.error(
                    f"    CRITICAL: Date range {current_start} to {chunk_end_dt} exceeds 30-day limit ({seconds_diff} seconds > {MAX_SECONDS})"
                )
                # Adjust to exactly 30 days if we somehow exceeded
                until_ts = since_ts + MAX_SECONDS
                chunk_end_dt = datetime.fromtimestamp(
                    until_ts, tz=timezone.utc
                ).date() - timedelta(days=1)

            # follower_count metric has RECENCY requirement: only available for last 30 days excluding today
            # API error: "(#100) (follower_count) metric only supports querying data for the last 30 days excluding the current day"
            today = date.today()
            earliest_follower_count_date = today - timedelta(days=30)

            # Exclude follower_count if current_start is older than 30 days ago
            # (safer to exclude for entire chunk if any part is outside the 30-day window)
            can_include_follower_count = current_start >= earliest_follower_count_date

            # Filter out follower_count for historical data
            metrics_filtered = [
                m
                for m in metrics_to_extract
                if m != "follower_count" or can_include_follower_count
            ]

            daily_data = {}  # date_str -> {metric: value}

            try:
                # Single API call: reach, follower_count (no metric_type needed, multi-day ranges)
                if metrics_filtered:
                    _rate_limit()
                    params = {
                        "metric": ",".join(metrics_filtered),
                        "period": period,
                        "since": since_ts,
                        "until": until_ts,
                    }
                    insights = ig_user.get_insights(params=params)
                    for insight in insights:
                        insight_dict = dict(insight)
                        metric_name = insight_dict.get("name", "")
                        values = insight_dict.get("values", [])
                        for val_entry in values:
                            end_time = val_entry.get("end_time", "")
                            value = val_entry.get("value", 0)
                            if end_time not in daily_data:
                                daily_data[end_time] = {}
                            daily_data[end_time][metric_name] = str(value)

                for date_str, metric_values in daily_data.items():
                    record = {
                        "ig_user_id": ig_user_id,
                        "username": username,
                        "date": date_str,
                        "page_id": acct.get("page_id", ""),
                        "account_id": account_id,
                    }
                    for m in metrics:
                        if m == "follower_count":
                            # Store the daily delta as follower_count_change
                            record["follower_count_change"] = metric_values.get(m, "0")
                            # Placeholder — snapshot will be stamped on most recent day below
                            record["follower_count"] = "0"
                        else:
                            record[m] = metric_values.get(m, "0")

                    account_records.append(record)
                    total_records += 1

            except FacebookRequestError as e:
                logger.warning(
                    f"    Account insights chunk {current_start} to {chunk_end_dt} failed: {e}"
                )
            except Exception as e:
                logger.warning(f"    Unexpected error for account insights chunk: {e}")

            # Move to next chunk (advance to day after chunk_end_dt)
            current_start = chunk_end_dt + timedelta(days=1)

            logger.debug(
                f"    Chunk {current_start - timedelta(days=1)} to {chunk_end_dt} complete. Next chunk starts: {current_start}"
            )

        # Stamp the absolute followers_count snapshot onto ALL records for this account.
        # The snapshot represents the current total — we backfill it so every day has
        # the best-known total rather than leaving most days as 0.
        # For historical accuracy, we could reconstruct past totals by subtracting
        # cumulative daily deltas, but the snapshot is far more useful than 0.
        if account_records and followers_count_snapshot is not None:
            # Sort by date to find the most recent record
            account_records.sort(key=lambda r: r.get("date", ""))

            # Reconstruct historical totals by walking backwards from the snapshot
            # using the daily deltas: day[N-1] = day[N] - delta[N]
            running_total = int(followers_count_snapshot)
            for i in range(len(account_records) - 1, -1, -1):
                account_records[i]["follower_count"] = str(running_total)
                daily_delta = int(account_records[i].get("follower_count_change", "0"))
                running_total = running_total - daily_delta

        for record in account_records:
            yield record

        logger.info(
            f"    Extracted {total_records} daily insight records for @{username}"
        )


def extract_stories(
    ig_accounts: List[Dict[str, Any]], account_id: str, test_mode: bool = False
) -> Iterator[Dict[str, Any]]:
    """Extract currently-active Instagram Stories (24hr window only)."""
    fields = [
        "id",
        "caption",
        "media_type",
        "media_url",
        "permalink",
        "thumbnail_url",
        "timestamp",
    ]

    for acct in ig_accounts:
        ig_user_id = acct["ig_user_id"]
        logger.info(f"  Extracting stories for @{acct['username']}")

        try:
            ig_user = IGUser(ig_user_id)
            _rate_limit()

            stories = ig_user.get_stories(fields=fields)

            count = 0
            for story_obj in stories:
                story_dict = dict(story_obj)

                record = {
                    "id": str(story_dict.get("id", "")),
                    "ig_user_id": ig_user_id,
                    "caption": story_dict.get("caption", ""),
                    "media_type": story_dict.get("media_type", ""),
                    "media_url": story_dict.get("media_url", ""),
                    "permalink": story_dict.get("permalink", ""),
                    "thumbnail_url": story_dict.get("thumbnail_url", ""),
                    "timestamp": story_dict.get("timestamp", ""),
                    "page_id": acct.get("page_id", ""),
                    "account_id": account_id,
                }
                yield record
                count += 1

            logger.info(f"    Found {count} active stories for @{acct['username']}")

        except FacebookRequestError as e:
            logger.error(f"    Stories extraction failed for @{acct['username']}: {e}")
        except Exception as e:
            logger.error(
                f"    Unexpected error extracting stories for @{acct['username']}: {e}"
            )


def extract_story_insights(
    ig_accounts: List[Dict[str, Any]],
    story_records: List[Dict[str, Any]],
    config: Dict[str, Any],
    account_id: str,
) -> Iterator[Dict[str, Any]]:
    """Extract per-story engagement insights."""
    table_config = config.get("resources", {}).get("story_insights", {})
    # v22+: Only reach and replies are supported for story insights
    # DEPRECATED: impressions, navigation, total_interactions, saved, exits, taps_forward, taps_back
    metrics = table_config.get("params", {}).get("metric", ["reach", "replies"])

    for story_rec in story_records:
        story_id = story_rec.get("id")
        ig_user_id = story_rec.get("ig_user_id", "")

        if not story_id:
            continue

        try:
            _rate_limit()
            ig_media = IGMedia(story_id)
            insights = ig_media.get_insights(params={"metric": ",".join(metrics)})

            record = {
                "story_id": str(story_id),
                "ig_user_id": ig_user_id,
                "timestamp": story_rec.get("timestamp", ""),
                "page_id": story_rec.get("page_id", ""),
                "account_id": account_id,
            }

            for insight in insights:
                insight_dict = dict(insight)
                metric_name = insight_dict.get("name", "")
                values = insight_dict.get("values", [])
                if values and isinstance(values, list) and len(values) > 0:
                    record[metric_name] = str(values[0].get("value", 0))
                else:
                    record[metric_name] = "0"

            yield record

        except FacebookRequestError as e:
            # Error code 10 = "Not enough viewers" - expected for low-engagement stories
            error_code = (
                getattr(e, "api_error_code", lambda: None)()
                if hasattr(e, "api_error_code")
                else None
            )
            if error_code == 10:
                logger.debug(
                    f"    Story {story_id} has insufficient viewers for insights (expected)"
                )
            else:
                logger.warning(f"    Story insights failed for story {story_id}: {e}")
        except Exception as e:
            logger.warning(
                f"    Unexpected error getting story insights for {story_id}: {e}"
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
    """Common helper to add metadata, normalize timestamps, and write records to Parquet.

    Returns (local_file_path, row_count) or (None, 0).
    """
    if not records:
        return None, 0

    if timestamp_fields is None:
        timestamp_fields = ["timestamp", "date"]

    # Add metadata
    set_execution_metadata(records, exec_id, source=source)

    for record in records:
        # Normalize timestamps
        for field in timestamp_fields:
            if field in record and record[field] is not None:
                record[field] = parse_facebook_timestamp(record[field])

        # Ensure string IDs
        record.update(ensure_string_ids(record))

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
    production_table_id = f"{env.project}.{main_dataset}.{table_name_with_affix}"

    # Write to local Parquet
    local_file_path = extract_to_local_parquet(
        data=records,
        source="instagram",
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

    # Initialize API (same as Facebook — shared Meta tokens)
    if not initialize_api(config):
        raise Exception("Failed to initialize Instagram API")

    # Handle site/account specification
    if not sites or sites == [] or sites == [""] or sites == [None]:
        account_id = os.getenv("FACEBOOK_ACCOUNT_ID")
        if account_id:
            sites = (
                [aid.strip() for aid in account_id.split(",")]
                if "," in account_id
                else [account_id]
            )
            logger.info(f"Using Facebook account(s) from environment: {sites}")
        else:
            logger.error("No FACEBOOK_ACCOUNT_ID found in environment")
            return {"total_rows": 0, "sites": 0, "tables": 0}
    elif sites == ["all"]:
        sites = get_available_sites(config)
        if not sites:
            logger.error("No accounts found")
            return {"total_rows": 0, "sites": 0, "tables": 0}

    # Get tables if not specified
    if not tables:
        tables = get_available_tables(config, group)
        if not tables:
            logger.error("No tables specified or found in group")
            return {"total_rows": 0, "sites": 0, "tables": 0}

    logger.info(f"Instagram pipeline: extracting {len(tables)} tables")

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
        start_dt = (
            date.min
        )  # No artificial cutoff — paginate through all available data
        date_range = (start_dt, end_dt)
        logger.info(
            f"FULL REFRESH MODE: Extracting ALL available data (no date cutoff)"
        )
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
        source_default="instagram",
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
    # The plugin still does its own record SHAPING and Parquet write via
    # _process_records_to_parquet; the accumulator owns the stats/skip/fail/return
    # so the contract (logical keys, no None paths, uniform status) is structural.
    from shared.extraction_result import StandardExtractionResult

    result = StandardExtractionResult(
        source=source,
        execution_id=exec_id,
        job_id=exec_id,
        bq_client=bq_client,
        sites=sites,
    )

    # account_id metadata always comes from the env var (canonical source),
    # never from --sites (which on Instagram is a username filter, not an ID).
    primary_account_id = os.getenv("FACEBOOK_ACCOUNT_ID", "").split(",")[0].strip()

    # Step 1: Discover Instagram Business accounts from Facebook Pages
    logger.info("Discovering Instagram Business accounts...")
    ig_accounts = discover_ig_accounts(config)

    if not ig_accounts:
        logger.warning("No Instagram Business accounts found linked to Facebook Pages")
        return {"total_rows": 0, "sites": 0, "tables": 0, "table_files": {}}

    # On Instagram, --sites is a username filter (IG has no FB-style account IDs).
    # When the user passes specific usernames, restrict discovery to those accounts.
    # Sentinel values 'all' and the env-var-derived FB account IDs are treated as no-filter.
    fb_account_ids = set(os.getenv("FACEBOOK_ACCOUNT_ID", "").split(","))
    username_filter = {
        s.lower().lstrip("@")
        for s in (sites or [])
        if s and s != "all" and s not in fb_account_ids
    }
    if username_filter:
        before = len(ig_accounts)
        ig_accounts = [
            a for a in ig_accounts if a.get("username", "").lower() in username_filter
        ]
        logger.info(
            f"Filtered IG accounts by username: {before} -> {len(ig_accounts)} (filter: {sorted(username_filter)})"
        )
        missing = username_filter - {a.get("username", "").lower() for a in ig_accounts}
        if missing:
            logger.warning(
                f"Requested usernames not found in discovered IG accounts: {sorted(missing)}"
            )
        if not ig_accounts:
            logger.error(
                "No IG accounts matched the username filter; nothing to extract"
            )
            return {"total_rows": 0, "sites": 0, "tables": 0, "table_files": {}}

    # Cache for dependent tables
    media_cache = []
    story_cache = []

    for table in tables:
        table_config = config.get("resources", {}).get(table, {})
        if not table_config.get("active", True):
            logger.info(f"Table '{table}' is marked as inactive, skipping")
            continue

        logger.info(f"\nProcessing table: {table}")

        try:
            if table == "ig_accounts":
                records = list(extract_ig_accounts(ig_accounts, primary_account_id))
                logger.info(f"  Found {len(records)} IG accounts")

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
                )

            elif table == "media":
                records = list(
                    extract_media(
                        ig_accounts, date_range, primary_account_id, test_mode
                    )
                )
                media_cache = records  # Cache for media_insights
                logger.info(f"  Found {len(records)} media items")

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
                )

            elif table == "media_insights":
                # Use cached media records, or extract fresh if media wasn't in the table list
                if not media_cache:
                    logger.info("  Media cache empty, extracting media first...")
                    media_cache = list(
                        extract_media(
                            ig_accounts, date_range, primary_account_id, test_mode
                        )
                    )

                # If no media records found, skip media_insights (valid state for incremental runs)
                if not media_cache:
                    logger.info(
                        "  No media records found - skipping media_insights extraction (valid for incremental runs with no new posts)"
                    )
                    result.skip_table(table, reason="zero_rows")
                    continue

                records = list(
                    extract_media_insights(
                        ig_accounts, media_cache, config, primary_account_id
                    )
                )
                logger.info(f"  Found {len(records)} media insight records")

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
                )

            elif table == "account_insights":
                records = list(
                    extract_account_insights(
                        ig_accounts, date_range, config, primary_account_id
                    )
                )
                logger.info(f"  Found {len(records)} account insight records")

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
                    timestamp_fields=["date"],
                )

            elif table == "stories":
                records = list(
                    extract_stories(ig_accounts, primary_account_id, test_mode)
                )
                story_cache = records  # Cache for story_insights
                logger.info(f"  Found {len(records)} active stories")

                # Stories expire after 24h; zero active stories is a valid steady state, not a failure
                if not records:
                    logger.info(
                        "  No active stories found - skipping stories table (stories expire after 24 hours)"
                    )
                    result.skip_table(table, reason="zero_rows")
                    continue

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
                )

            elif table == "story_insights":
                if not story_cache:
                    logger.info("  Story cache empty, extracting stories first...")
                    story_cache = list(
                        extract_stories(ig_accounts, primary_account_id, test_mode)
                    )

                # If no stories found, skip story_insights (valid state - stories expire after 24 hours)
                if not story_cache:
                    logger.info(
                        "  No active stories found - skipping story_insights extraction (stories expire after 24 hours)"
                    )
                    result.skip_table(table, reason="zero_rows")
                    continue

                records = list(
                    extract_story_insights(
                        ig_accounts, story_cache, config, primary_account_id
                    )
                )
                logger.info(f"  Found {len(records)} story insight records")

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
                )

            else:
                logger.warning(f"  Unknown table type: {table}")
                continue

            # The plugin already wrote the Parquet (_process_records_to_parquet);
            # record the existing path + count. A falsy local_file here means the
            # table genuinely returned zero rows -> recorded as zero_rows (a valid
            # steady state), NOT a failure (the old code mislabeled it failed).
            if local_file:
                logger.info(f"  Parquet file created: {local_file} ({row_count} rows)")
            else:
                logger.info(f"  No rows for {table} (recorded as zero_rows)")
            result.record_prewritten(table, local_file, row_count or 0)

        except Exception as e:
            logger.error(f"  {table} extraction failed: {e}")
            result.fail_table(
                table, error=str(e), stage="extract", error_type=type(e).__name__
            )

        logger.info(f"Table {table} complete")

    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("INSTAGRAM PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total rows extracted: {result.total_rows}")
    logger.info(f"Tables processed: {len(tables)}")
    logger.info(f"  Successful: {result.successful_tables}")
    logger.info(f"  Failed: {result.failed_tables}")
    logger.info(f"IG accounts processed: {len(ig_accounts)}")
    logger.info("=" * 60)

    return result.finalize()
