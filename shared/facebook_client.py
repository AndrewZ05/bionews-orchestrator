"""Facebook API client helpers shared across extractors."""

from __future__ import annotations

import copy
import json
import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterator, List, Tuple

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.exceptions import FacebookRequestError

from shared.rate_limiter import (
    handle_rate_limit_error,
    get_rate_limiter,
    reset_rate_limit,
    wait_if_needed,
    with_rate_limit,
)

logger = logging.getLogger(__name__)


FACEBOOK_RATE_LIMITS: Dict[str, int] = {
    "default": 200,
    "insights": 100,
    "async": 50,
    "campaigns": 180,
    "adsets": 180,
    "ads": 180,
    "adcreatives": 150,
    "leads": 100,
    "account_insights": 80,
    "ad_insights": 80,
}


RESOURCE_CONFIG: Dict[str, Dict[str, Any]] = {
    "campaigns": {
        "object": "AdAccount",
        "method": "get_campaigns",
        "fields": [
            "id",
            "name",
            "account_id",
            "status",
            "effective_status",
            "objective",
            "created_time",
            "updated_time",
            "start_time",
            "stop_time",
            "daily_budget",
            "lifetime_budget",
            "bid_strategy",
        ],
        "primary_key": ["account_id", "id"],
    },
    "adsets": {
        "object": "AdAccount",
        "method": "get_ad_sets",
        "fields": [
            "id",
            "name",
            "account_id",
            "campaign_id",
            "status",
            "effective_status",
            "targeting",
            "daily_budget",
            "lifetime_budget",
            "bid_amount",
            "bid_strategy",
            "billing_event",
            "optimization_goal",
            "created_time",
            "updated_time",
            "start_time",
            "end_time",
        ],
        "primary_key": ["account_id", "id"],
    },
    "ads": {
        "object": "AdAccount",
        "method": "get_ads",
        "fields": [
            "id",
            "name",
            "account_id",
            "campaign_id",
            "adset_id",
            "status",
            "effective_status",
            "creative",  # Changed: nested field expansion no longer allowed in Graph API v25.0+
            "created_time",
            "updated_time",
            "bid_amount",
            "bid_type",
            "configured_status",
        ],
        "primary_key": ["account_id", "id"],
    },
    "adcreatives": {
        "object": "AdAccount",
        "method": "get_ad_creatives",
        "fields": [
            "id",
            "name",
            "status",
            "body",
            "title",
            "link_url",
            "call_to_action_type",
            "object_type",
            "object_story_spec",
        ],
        "primary_key": ["account_id", "id"],
    },
    "adimages": {
        "object": "AdAccount",
        "method": "get_ad_images",
        "fields": [
            "id",
            "name",
            "hash",
            "url",
            "url_128",
            "permalink_url",
            "original_width",
            "original_height",
            "created_time",
            "updated_time",
            "bytes",
            "status",
        ],
        "primary_key": ["account_id", "id"],
    },
    "customaudiences": {
        "object": "AdAccount",
        "method": "get_custom_audiences",
        "fields": [
            "id",
            "name",
            "subtype",
            "delivery_status",
            "description",
            "operation_status",
            "time_created",
            "time_updated",
        ],
        "primary_key": ["account_id", "id"],
    },
    "adaccounts": {
        "object": "AdAccount",
        "method": "api_get",
        "fields": [
            "id",
            "account_id",
            "name",
            "account_status",
            "age",
            "amount_spent",
            "balance",
            "business_name",
            "business_city",
            "business_country_code",
            "created_time",
            "currency",
            "spend_cap",
            "timezone_name",
        ],
        "primary_key": ["account_id"],
    },
}


def get_facebook_rate_limit(endpoint_type: str = "default") -> float:
    calls_per_hour = FACEBOOK_RATE_LIMITS.get(endpoint_type, 200)
    return calls_per_hour / 3600


def wait_for_facebook(endpoint_type: str = "default") -> float:
    calls_per_second = get_facebook_rate_limit(endpoint_type)
    limiter_key = f"facebook_{endpoint_type}"
    get_rate_limiter(limiter_key, calls_per_second=calls_per_second)
    return wait_if_needed(limiter_key)


# Facebook signals a retryable condition several ways: an explicit is_transient
# flag, the temporary-availability error (code 2), and the app/user rate-limit
# errors (codes 4 and 17). Any of these on an async insights job means a fresh
# submission after a short wait is likely to succeed -- so we resubmit rather
# than fall back to the synchronous path (which has no async worker behind it
# and is far more likely to hit the same rate limit).
_TRANSIENT_ERROR_CODES = {2, 4, 17}
_TRANSIENT_ERROR_TEXT = (
    "request limit reached",
    "temporarily unavailable",
    "too many api requests",
    "try again",
)


def _is_transient_async_error(error_json: Any) -> bool:
    """Return True if an async-job error payload looks retryable.

    Decision order: an explicit is_transient flag wins (True retries, False
    forces non-transient regardless of text). Otherwise classify by error code,
    then by matching the human-readable message text only -- never the
    serialized dict, whose key names ("is_transient", "error_code") would
    otherwise produce false positives.
    """
    if isinstance(error_json, dict):
        if "is_transient" in error_json:
            return bool(error_json["is_transient"])
        if error_json.get("error_code") in _TRANSIENT_ERROR_CODES:
            return True
        message_parts = [
            str(error_json.get(key, ""))
            for key in (
                "error_message",
                "message",
                "error_user_msg",
                "error_user_title",
            )
        ]
        text = " ".join(message_parts).lower()
    else:
        text = str(error_json or "").lower()
    return any(token in text for token in _TRANSIENT_ERROR_TEXT)


@with_rate_limit(source="facebook_hierarchy", calls_per_second=0.05)
def fetch_resource_batch(
    account_id: str,
    resource_name: str,
    fields: List[str],
    params: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Fetch a batch of resources from Facebook API with retry-aware rate limiting."""

    if not account_id.startswith("act_"):
        account_id = f"act_{account_id}"

    wait_for_facebook(resource_name)

    account = AdAccount(account_id)
    resource_config = RESOURCE_CONFIG[resource_name]
    method = getattr(account, resource_config["method"])

    try:
        items = method(fields=fields, params=params)
        result: List[Dict[str, Any]] = [dict(item) for item in items]
        logger.debug(
            "Fetched %s %s records for %s (params=%s)",
            len(result),
            resource_name,
            account_id,
            params,
        )
        reset_rate_limit(f"facebook_{resource_name}")
        return result
    except FacebookRequestError as exc:
        if exc.api_error_code() in {17, 4} or "User request limit reached" in str(exc):
            handle_rate_limit_error(f"facebook_{resource_name}", exc)
        raise


def wait_for_async_jobs(
    jobs: List[Dict[str, Any]],
    *,
    max_wait_minutes: int = 30,
) -> List[Dict[str, Any]]:
    """Poll async insights export jobs until completion."""

    start_time = time.time()
    max_wait_seconds = max_wait_minutes * 60

    last_log_time = 0.0
    last_jobs_complete = -1
    last_max_percent = -1

    while True:
        all_complete = True
        jobs_complete = 0
        max_percent = 0

        for job in jobs:
            async_job: AdReportRun = job.get("async_job")
            if not async_job:
                continue

            try:
                async_job.api_get()
                status_raw = str(async_job.get("async_status", "")).strip()
                status = status_raw.lower()
                percent_complete = async_job.get("async_percent_completion", 0)

                job["status"] = status
                job["percent_complete"] = percent_complete
                try:
                    pct_val = float(percent_complete)
                except (TypeError, ValueError):
                    pct_val = 0.0
                if pct_val > max_percent:
                    max_percent = pct_val

                previous_status = job.get("status_prev")
                if previous_status != status:
                    job["status_prev"] = status
                    logger.info(
                        "Async insights job %s status: %s (%.0f%%)",
                        job.get("job_id", job.get("chunk_num")),
                        status_raw or status,
                        pct_val,
                    )

                if "completed" in status:
                    jobs_complete += 1
                    job["status_normalized"] = "completed"
                elif any(
                    flag in status for flag in ("canceled", "cancelled", "failed")
                ):
                    jobs_complete += 1
                    job["status_normalized"] = "failed"
                    job["error"] = json.dumps(async_job._json)
                else:
                    job["status_normalized"] = "running"
                    all_complete = False
            except FacebookRequestError as exc:
                handle_rate_limit_error("facebook_async_status", exc)
                all_complete = False

        elapsed_minutes = (time.time() - start_time) / 60
        now = time.time()
        should_log = (
            jobs_complete != last_jobs_complete
            or max_percent != last_max_percent
            or last_log_time == 0.0
            or (now - last_log_time) >= 600
        )

        if should_log:
            logger.info(
                "Async insights progress: %s/%s complete (max %.0f%%, elapsed %.1f min)",
                jobs_complete,
                len(jobs),
                max_percent,
                elapsed_minutes,
            )
            last_log_time = now
            last_jobs_complete = jobs_complete
            last_max_percent = max_percent

        if all_complete:
            break

        if (time.time() - start_time) > max_wait_seconds:
            logger.warning(
                "Timed out waiting for async jobs (%s minutes)", max_wait_minutes
            )
            break

        time.sleep(30)

    summary = []
    for job in jobs:
        status_label = job.get("status_normalized", job.get("status", "unknown"))
        try:
            pct_value = float(job.get("percent_complete", 0) or 0)
        except (TypeError, ValueError):
            pct_value = 0.0
        summary.append(
            f"{job.get('job_id')}: {status_label.upper()} ({pct_value:.0f}%)"
        )
    if summary:
        logger.info("Async insights jobs finished: %s", "; ".join(summary))

    return jobs


def extract_insights_async(
    account_id: str,
    table_config: Dict[str, Any],
    date_range: Tuple[date, date],
    *,
    level: str = "ad",
    test_mode: bool = False,
) -> Iterator[Dict[str, Any]]:
    """Universal async insights extraction with chunked polling."""

    if not account_id.startswith("act_"):
        account_id = f"act_{account_id}"

    start_date, end_date = date_range

    if start_date is None or end_date is None:
        logger.warning("No date range provided for insights extraction; skipping.")
        return

    if start_date > end_date:
        start_date, end_date = end_date, start_date

    total_days = (end_date - start_date).days + 1

    if level == "account":
        chunk_days = 180 if total_days > 365 else total_days
    elif total_days > 365:
        chunk_days = 90
    elif total_days > 90:
        chunk_days = 30
    else:
        chunk_days = total_days

    chunk_days = max(chunk_days, 1)

    current_date = start_date
    chunk_count = 0
    total_chunks = max((total_days + chunk_days - 1) // chunk_days, 1)

    jobs: List[Dict[str, Any]] = []

    while current_date <= end_date:
        chunk_end = min(current_date + timedelta(days=chunk_days - 1), end_date)
        chunk_count += 1

        account = AdAccount(account_id)
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

        if level == "account":
            fields = [
                f
                for f in fields
                if not any(prefix in f for prefix in ("campaign_", "adset_", "ad_"))
            ]
        elif level == "campaign":
            fields = [
                f
                for f in fields
                if not any(prefix in f for prefix in ("adset_", "ad_"))
            ]
        elif level == "adset":
            fields = [f for f in fields if f != "ad_name"]

        async_job = account.get_insights(fields=fields, params=params, is_async=True)
        job_id = str(async_job["id"])
        logger.info(
            "Created async insights job %s/%s for %s (%s -> %s)",
            chunk_count,
            total_chunks,
            account_id,
            current_date,
            chunk_end,
        )

        jobs.append(
            {
                "job_id": job_id,
                "async_job": async_job,
                "chunk_num": chunk_count,
                "date_range": (current_date, chunk_end),
                "status": "Job Running",
                "percent_complete": 0,
                "params": copy.deepcopy(params),
                "fields": list(fields),
                "level": level,
                "account_id": account_id,
            }
        )

        current_date = chunk_end + timedelta(days=1)
        if test_mode and chunk_count >= 2:
            break

    if not jobs:
        logger.warning(
            "No insights jobs were created for %s (%s -> %s)",
            account_id,
            start_date,
            end_date,
        )
        return

    completed = wait_for_async_jobs(jobs)

    def format_insight_record(insight_obj: Any) -> Dict[str, Any]:
        insight_data = dict(insight_obj)
        insight_data["account_id"] = account_id

        for ts_field in ("date_start", "date_stop"):
            if ts_field in insight_data and isinstance(insight_data[ts_field], str):
                from dateutil import parser

                try:
                    insight_data[ts_field] = parser.parse(insight_data[ts_field])
                except Exception as exc:  # pragma: no cover
                    logger.debug(
                        "Failed to parse %s=%s (%s)",
                        ts_field,
                        insight_data[ts_field],
                        exc,
                    )

        complex_fields = [
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
        ]

        for field in complex_fields:
            if field not in insight_data or not insight_data[field]:
                continue
            value = insight_data[field]
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and "action_type" in item:
                        metric_name = item["action_type"].replace(".", "_")
                        metric_value = item.get("value", 0)
                        try:
                            metric_value = float(metric_value) if metric_value else 0.0
                        except (ValueError, TypeError):
                            metric_value = 0.0
                        insight_data[f"{field}_{metric_name}"] = metric_value
                insight_data[f"{field}_json"] = json.dumps(value)
            else:
                insight_data[f"{field}_json"] = json.dumps(value)

        return insight_data

    # Retry transient async-job failures by resubmitting a fresh async job
    # before resorting to the synchronous fallback. Service-unavailable (code 2)
    # and rate-limit (codes 4/17) failures usually clear after a short wait, and
    # the async worker is far cheaper on the app's request budget than the sync
    # path -- which is exactly what was already exhausted when we got here.
    max_async_retries = 2
    for retry_attempt in range(1, max_async_retries + 1):
        retry_targets = []
        for job_info in completed:
            if job_info.get("status_normalized") != "failed":
                continue
            error_json = job_info.get("error")
            try:
                parsed = (
                    json.loads(error_json)
                    if isinstance(error_json, str)
                    else error_json
                )
            except (TypeError, ValueError):
                parsed = error_json
            inner = parsed.get("error", parsed) if isinstance(parsed, dict) else parsed
            if _is_transient_async_error(inner):
                retry_targets.append(job_info)

        if not retry_targets:
            break

        backoff = handle_rate_limit_error(
            "facebook_async_insights", Exception("transient async failure")
        )
        logger.warning(
            "Resubmitting %d transient async insights job(s), attempt %d/%d after %.0fs backoff",
            len(retry_targets),
            retry_attempt,
            max_async_retries,
            backoff,
        )
        time.sleep(backoff)

        resubmitted = []
        for job_info in retry_targets:
            try:
                acct = AdAccount(job_info.get("account_id", account_id))
                new_job = acct.get_insights(
                    fields=list(job_info.get("fields") or fields),
                    params=copy.deepcopy(job_info.get("params", {})),
                    is_async=True,
                )
                job_info["async_job"] = new_job
                job_info["job_id"] = str(new_job["id"])
                job_info["status_normalized"] = "running"
                job_info["status_prev"] = None
                job_info["error"] = None
                resubmitted.append(job_info)
            except Exception as resubmit_err:  # pragma: no cover - network dependent
                logger.warning(
                    "Failed to resubmit async insights job: %s",
                    str(resubmit_err)[:200],
                )

        if not resubmitted:
            break
        wait_for_async_jobs(resubmitted)

    reset_rate_limit("facebook_async_insights")

    completed_jobs: List[Dict[str, Any]] = []
    fallback_jobs: List[Dict[str, Any]] = []

    for job_info in completed:
        status = job_info.get("status_normalized")
        if status == "completed":
            completed_jobs.append(job_info)
        else:
            if status == "failed" and job_info.get("error"):
                logger.warning(
                    "Async insights job %s failed: %s",
                    job_info.get("job_id"),
                    job_info.get("error"),
                )
            fallback_jobs.append(job_info)

    for job_info in completed_jobs:
        async_job = job_info["async_job"]
        for attempt in range(1, 4):
            try:
                cursor = async_job.get_insights()
                for insight in cursor:
                    yield format_insight_record(insight)
                break  # Cursor fully consumed
            except GeneratorExit:
                raise
            except Exception as cursor_err:
                error_str = str(cursor_err)
                is_transient = any(
                    s in error_str
                    for s in ["500", "ConnectionError", "Connection aborted"]
                )
                if is_transient and attempt < 3:
                    wait_secs = 30 * attempt
                    logger.warning(
                        "Cursor pagination error on attempt %d/3, retrying in %ds: %s",
                        attempt,
                        wait_secs,
                        error_str[:120],
                    )
                    time.sleep(wait_secs)
                else:
                    raise

    if fallback_jobs:
        logger.warning(
            "Falling back to synchronous insights extraction for %d chunk(s)",
            len(fallback_jobs),
        )
        account = AdAccount(account_id)
        for job_info in fallback_jobs:
            params = copy.deepcopy(job_info.get("params", {}))
            fields_for_job = list(job_info.get("fields") or fields)
            # The sync fallback runs precisely when the async path already
            # failed -- often because the app's request budget is exhausted. It
            # must therefore handle rate limits itself rather than crash the
            # whole generator. Buffer each chunk before yielding so a mid-cursor
            # retry cannot emit duplicate rows.
            max_sync_attempts = 3
            for attempt in range(1, max_sync_attempts + 1):
                try:
                    wait_for_facebook("insights")
                    cursor = account.get_insights(fields=fields_for_job, params=params)
                    chunk_records = [
                        format_insight_record(insight) for insight in cursor
                    ]
                    reset_rate_limit("facebook_insights")
                    for record in chunk_records:
                        yield record
                    job_info["status_normalized"] = "fallback"
                    job_info["percent_complete"] = 100
                    break
                except GeneratorExit:
                    raise
                except FacebookRequestError as exc:
                    is_rate_limit = (
                        exc.api_error_code() in _TRANSIENT_ERROR_CODES
                        or _is_transient_async_error(str(exc))
                    )
                    if is_rate_limit and attempt < max_sync_attempts:
                        backoff = handle_rate_limit_error("facebook_insights", exc)
                        logger.warning(
                            "Sync insights fallback rate-limited (attempt %d/%d), backing off %.0fs",
                            attempt,
                            max_sync_attempts,
                            backoff,
                        )
                        time.sleep(backoff)
                        continue
                    logger.error(
                        "Sync insights fallback failed for chunk %s after %d attempt(s): %s",
                        job_info.get("chunk_num"),
                        attempt,
                        str(exc)[:200],
                    )
                    job_info["status_normalized"] = "failed"
                    break


__all__ = [
    "FACEBOOK_RATE_LIMITS",
    "RESOURCE_CONFIG",
    "get_facebook_rate_limit",
    "wait_for_facebook",
    "fetch_resource_batch",
    "wait_for_async_jobs",
    "extract_insights_async",
]
