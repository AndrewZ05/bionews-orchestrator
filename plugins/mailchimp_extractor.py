#!/usr/bin/env python3
# Mailchimp extractor plugin (functional implementation)

import os
import shutil
import json
import time
import csv
import io
import logging
import uuid
import hashlib
import tempfile
import zipfile
import math
import threading
import glob
from pathlib import Path
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, date
from typing import Dict, Any, List, Optional, Tuple, Set, Callable, TYPE_CHECKING
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.types as pa_types

from shared.rate_limiter import wait_if_needed
from shared.extractor_utils import apply_table_affix, get_available_tables
from shared.account_context import (
    enrich_record_with_tenant_info,
    set_execution_metadata,
)
from shared.mailchimp_client import (
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_BATCH_POLL_INTERVAL_SECONDS,
    DEFAULT_BATCH_TIMEOUT_SECONDS,
    MAILCHIMP_RATE_LIMITER,
    MAX_BATCH_OPERATIONS,
    MAILCHIMP_MAX_CONCURRENT_REQUESTS,
    batch_fetch_paginated_collection,
    build_batch_operation,
    build_batch_operation_path,
    create_mailchimp_session,
    configure_mailchimp_rate_limit,
    execute_mailchimp_batch,
    fetch_paginated_collection,
    mailchimp_request,
)
from shared.mailchimp_cache import (
    MailchimpMetadataManager,
    filter_preloaded_campaigns,
    merge_records_by_key,
    resolve_run_window,
)
from shared.schema_discovery import discover_schema_with_pyarrow_duckdb
from shared.gcs_pipeline import read_yaml_schema

DEFAULT_MEMBER_MIN_PAGE_SIZE = 100
DEFAULT_MEMBER_PAGE_SIZE_STEP = 100
DEFAULT_BULK_POLL_INTERVAL_SECONDS = 10
DEFAULT_BULK_TIMEOUT_SECONDS = 3600
DEFAULT_BULK_EXISTING_WAIT_SECONDS = 600

BULK_SUPPORTED_TABLES = {"unsubscribes", "bounces"}

# Campaign tables that can be extracted together
CAMPAIGN_REPORT_TABLES = {
    "campaign_sent_to",
    "campaign_email_activity",
    "campaign_open_details",
    "campaign_click_details",
    "campaign_click_members",
    "campaign_domain_performance",
    "campaign_locations",
}

BATCH_ELIGIBLE_TABLES = {
    "campaign_sent_to",
    "campaign_email_activity",
    "campaign_open_details",
    "campaign_click_details",
    "campaign_click_members",
    "campaign_domain_performance",
    "campaign_locations",
}

JSONL_TO_PARQUET_CHUNK_SIZE = 1_000_000


_worker_session_local = threading.local()

if TYPE_CHECKING:
    from shared.extractor_runner import PipelineEnvironment

SYSTEM_FIELDS = {
    "row_hash",
    "loaded_at",
    "execution_id",
    "updated_at",
    "source",
}


def get_worker_mailchimp_session(
    api_key: str, request_timeout: int
) -> requests.Session:
    session = getattr(_worker_session_local, "session", None)
    if session is None:
        session = create_mailchimp_session(api_key, request_timeout)
        _worker_session_local.session = session
    return session


def _process_campaign_email_activity(
    campaign: Dict[str, Any],
    session: Optional[requests.Session],
    base_url: str,
    since: Optional[str],
    since_dt: Optional[datetime],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    api_key: Optional[str],
    request_timeout: int,
) -> List[Dict[str, Any]]:
    campaign_id = campaign.get("id")
    if not campaign_id:
        return []

    worker_session = session
    if worker_session is None:
        if not api_key:
            raise ValueError("API key required for worker session")
        worker_session = get_worker_mailchimp_session(api_key, request_timeout)

    # See _process_campaign_all_tables: the old min(page_size, 200) cap cost 5x
    # the requests for byte-identical data.
    params: Dict[str, Any] = {"count": page_size}
    if since:
        params["since"] = since
    params = _with_fields(params, "campaign_email_activity")

    emails = fetch_paginated_collection(
        worker_session,
        base_url,
        f"/reports/{campaign_id}/email-activity",
        "emails",
        params,
        page_size,
        max_attempts,
        backoff_seconds,
    )

    list_id = (campaign.get("recipients") or {}).get("list_id")
    rows: List[Dict[str, Any]] = []

    for email_entry in emails or []:
        email_address = email_entry.get("email_address")
        email_id = email_entry.get("email_id") or derive_email_id(
            campaign_id, email_address
        )
        activities = email_entry.get("activity", []) or []
        for activity_entry in activities:
            timestamp = activity_entry.get("timestamp")
            activity_dt = parse_mailchimp_timestamp(timestamp)

            if since_dt:
                if activity_dt and activity_dt < since_dt:
                    continue
                if activity_dt is None:
                    # Without a timestamp we cannot enforce the since window reliably.
                    continue

            if not activity_dt:
                continue

            action = activity_entry.get("action") or activity_entry.get("type")
            row = {
                "campaign_id": campaign_id,
                "list_id": list_id,
                "email_id": email_id,
                "email_address": email_address,
                "action": action,
                "activity_timestamp": activity_dt,
                "activity_type": activity_entry.get("type"),
                "ip": activity_entry.get("ip"),
                "url": activity_entry.get("url"),
                "device": activity_entry.get("device"),
                "user_agent": activity_entry.get("user_agent"),
            }

            geo_info = activity_entry.get("geo")
            if isinstance(geo_info, dict):
                row["geo_country"] = geo_info.get("country")
                row["geo_region"] = geo_info.get("region")
            else:
                row["geo_country"] = activity_entry.get("country")
                row["geo_region"] = activity_entry.get("region")

            rows.append(row)

    return rows


class MailchimpAPIError(RuntimeError):
    pass


class AccountExportInProgressError(RuntimeError):
    def __init__(self, export_id: Optional[str], payload: Any):
        message = "Mailchimp account export already in progress"
        super().__init__(message)
        self.export_id = export_id
        self.payload = payload


# Import centralized timestamp parsing utilities
from shared.timestamp_utils import parse_mailchimp_timestamp, parse_mailchimp_date


def _get_config_path(source_name: str) -> Path:
    return Path(__file__).resolve().parents[1] / "configs" / f"{source_name}.yaml"


def _load_effective_schema(
    source_name: str,
    base_table: str,
    table_with_affix: str,
    bq_client: bigquery.Client,
    env: "PipelineEnvironment",
) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str], Optional[Path]]:
    yaml_schema: Dict[str, str] = {}
    prod_schema: Dict[str, str] = {}
    config_path = _get_config_path(source_name)

    if config_path.exists():
        try:
            yaml_schema = read_yaml_schema(str(config_path), base_table) or {}
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "Failed reading YAML schema for %s: %s", base_table, exc
            )

    if bq_client:
        full_table_id = f"{env.project}.{env.production_dataset}.{table_with_affix}"
        try:
            prod_table = bq_client.get_table(full_table_id)
            prod_schema = {field.name: field.field_type for field in prod_table.schema}
        except NotFound:
            prod_schema = {}
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "Failed reading production schema for %s: %s", full_table_id, exc
            )

    # CRITICAL: YAML schema takes precedence over production schema
    # This matches the priority in gcs_pipeline.py: YAML > Production > DuckDB
    # YAML is authoritative because it defines all fields with explicit types
    effective_schema: Dict[str, str] = dict(yaml_schema or {})
    for column, data_type in prod_schema.items():
        effective_schema.setdefault(column, data_type)

    data_schema = {
        column: dtype
        for column, dtype in effective_schema.items()
        if column not in SYSTEM_FIELDS
    }

    return (
        data_schema,
        yaml_schema,
        prod_schema,
        config_path if config_path.exists() else None,
    )


def _map_bq_type_to_arrow(bq_type: str) -> pa.DataType:
    normalized = (bq_type or "STRING").upper()
    if normalized in {"STRING", "GEOGRAPHY", "JSON"}:
        return pa.string()
    if normalized in {"BYTES"}:
        return pa.binary()
    if normalized in {"INT64", "INTEGER", "INT", "BIGINT"}:
        return pa.int64()
    if normalized in {"FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC", "DECIMAL"}:
        return pa.float64()
    if normalized in {"BOOL", "BOOLEAN"}:
        return pa.bool_()
    if normalized in {"TIMESTAMP"}:
        return pa.timestamp("us", tz="UTC")
    if normalized in {"DATETIME"}:
        return pa.timestamp("us")
    if normalized in {"DATE"}:
        return pa.date32()
    if normalized in {"TIME"}:
        return pa.time64("us")
    return pa.string()


def _arrow_type_to_bq(arrow_type: pa.DataType) -> str:
    if pa_types.is_timestamp(arrow_type):
        unit = getattr(arrow_type, "unit", None)
        tz = getattr(arrow_type, "tz", None)
        return "TIMESTAMP" if tz else "DATETIME"
    if pa_types.is_date(arrow_type):
        return "DATE"
    if pa_types.is_time(arrow_type):
        return "TIME"
    if pa_types.is_integer(arrow_type):
        return "INT64"
    if pa_types.is_floating(arrow_type) or pa_types.is_decimal(arrow_type):
        return "FLOAT64"
    if pa_types.is_boolean(arrow_type):
        return "BOOLEAN"
    if pa_types.is_binary(arrow_type) or pa_types.is_fixed_size_binary(arrow_type):
        return "BYTES"
    return "STRING"


def _format_duckdb_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _build_duckdb_cast(
    column: str, source_expr: str, target_type: Optional[str]
) -> str:
    normalized = (target_type or "STRING").upper()

    if normalized in {"INT64", "INTEGER", "INT", "BIGINT"}:
        cast_type = "BIGINT"
    elif normalized in {
        "FLOAT64",
        "FLOAT",
        "NUMERIC",
        "BIGNUMERIC",
        "DECIMAL",
        "DOUBLE",
    }:
        cast_type = "DOUBLE"
    elif normalized in {"BOOL", "BOOLEAN"}:
        cast_type = "BOOLEAN"
    elif normalized == "TIMESTAMP":
        cast_type = "TIMESTAMP"
    elif normalized == "DATETIME":
        cast_type = "TIMESTAMP"
    elif normalized == "DATE":
        cast_type = "DATE"
    elif normalized == "TIME":
        cast_type = "TIME"
    elif normalized == "BYTES":
        cast_type = "BLOB"
    else:
        cast_type = "VARCHAR"

    return f'CAST({source_expr} AS {cast_type}) AS "{column}"'


def _coerce_value_for_type(value: Any, target_type: str) -> Any:
    if value is None:
        return None

    normalized = (target_type or "STRING").upper()

    if normalized in {"STRING", "GEOGRAPHY", "JSON"}:
        if isinstance(value, str):
            return value
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except Exception:
                return value.decode("latin-1", errors="ignore")
        if isinstance(value, (dict, list, set, tuple)):
            try:
                return json.dumps(value, default=str)
            except Exception:
                return str(value)
        return str(value)

    if normalized in {"INT64", "INTEGER", "INT", "BIGINT"}:
        try:
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, (int,)):
                return int(value)
            if isinstance(value, float):
                if math.isnan(value):
                    return None
                return int(value)
            if isinstance(value, str):
                cleaned = value.strip()
                if not cleaned:
                    return None
                return int(float(cleaned))
        except Exception:
            return None
        return None

    if normalized in {"FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC", "DECIMAL"}:
        try:
            if isinstance(value, bool):
                return float(int(value))
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                cleaned = value.strip()
                if not cleaned:
                    return None
                return float(cleaned)
        except Exception:
            return None
        return None

    if normalized in {"BOOL", "BOOLEAN"}:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes", "y", "t"}:
                return True
            if lowered in {"false", "0", "no", "n", "f"}:
                return False
        return None

    if normalized == "TIMESTAMP":
        parsed = parse_mailchimp_timestamp(value)
        return parsed

    if normalized == "DATETIME":
        return parse_mailchimp_timestamp(value)

    if normalized == "DATE":
        return parse_mailchimp_date(value)

    if normalized == "TIME":
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                return datetime.strptime(text[:26], "%H:%M:%S.%f").time()
            except ValueError:
                try:
                    return datetime.strptime(text[:8], "%H:%M:%S").time()
                except ValueError:
                    return None
        return None

    if normalized == "BYTES":
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
        return bytes(str(value), "utf-8")

    return value


def _coerce_records_to_schema(
    records: List[Dict[str, Any]], schema: Dict[str, str]
) -> None:
    for record in records:
        for column, target_type in schema.items():
            if column in record:
                record[column] = _coerce_value_for_type(record[column], target_type)
            else:
                record[column] = None

        for column in list(record.keys()):
            if column not in schema:
                record[column] = _coerce_value_for_type(record[column], "STRING")


def _infer_types_for_new_columns_from_records(
    records: List[Dict[str, Any]],
    known_schema: Dict[str, str],
    table_name: str,
    force_string_fields: Optional[List[str]] = None,
) -> Dict[str, str]:
    new_columns: Set[str] = set()
    for record in records:
        for column in record.keys():
            if column not in known_schema:
                new_columns.add(column)

    if not new_columns:
        return {}

    subset = []
    for record in records:
        row = {column: record.get(column) for column in new_columns if column in record}
        if row:
            subset.append(row)

    if not subset:
        return {}

    inferred = discover_schema_with_pyarrow_duckdb(
        subset, table_name, force_string_fields=force_string_fields
    )
    return {col: inferred.get(col, "STRING") for col in new_columns}


def _infer_types_for_new_columns_from_parquet(
    parquet_path: str,
    new_columns: Set[str],
    table_name: str,
    force_string_fields: Optional[List[str]] = None,
) -> Dict[str, str]:
    if not new_columns:
        return {}

    try:
        table = pq.read_table(parquet_path, columns=list(new_columns))
    except Exception as exc:
        logger.warning(
            "%s: Could not read parquet for type inference: %s", table_name, exc
        )
        return {}

    if table.num_rows == 0:
        return {}

    data = table.to_pylist()
    inferred = discover_schema_with_pyarrow_duckdb(
        data, table_name, force_string_fields=force_string_fields
    )
    return {col: inferred.get(col, "STRING") for col in new_columns}


def safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return int(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        cleaned = cleaned.replace(",", "")
        try:
            return int(cleaned)
        except ValueError:
            try:
                return int(float(cleaned))
            except ValueError:
                return None
    return None


def filter_campaigns_by_date_and_activity(
    campaigns: List[Dict[str, Any]],
    campaign_filters: Optional[Dict[str, Any]],
    logger: logging.Logger,
    skip_date_filtering: bool = False,
    explicit_start_date: Optional[str] = None,
    explicit_end_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Filter campaigns based on send date and activity thresholds.

    Args:
        skip_date_filtering: If True, skip config date filtering (when explicit dates provided)
        explicit_start_date: Command line start date (overrides config)
        explicit_end_date: Command line end date (for date range filtering)
    """
    if not campaign_filters and not explicit_start_date and not explicit_end_date:
        return campaigns

    min_send_date = campaign_filters.get("min_send_date") if campaign_filters else None
    lookback_days = campaign_filters.get("lookback_days") if campaign_filters else None
    min_emails_sent = (
        campaign_filters.get("min_emails_sent", 0) if campaign_filters else 0
    )

    # Determine date range
    cutoff_start_date = None
    cutoff_end_date = None

    # Use explicit dates if provided (command line override)
    if explicit_start_date:
        try:
            cutoff_start_date = parse_mailchimp_timestamp(
                f"{explicit_start_date}T00:00:00Z"
            )
        except Exception:
            logger.warning(
                f"Invalid explicit_start_date format: {explicit_start_date}, ignoring"
            )

    if explicit_end_date:
        try:
            # End date should be inclusive (end of day)
            cutoff_end_date = parse_mailchimp_timestamp(
                f"{explicit_end_date}T23:59:59Z"
            )
        except Exception:
            logger.warning(
                f"Invalid explicit_end_date format: {explicit_end_date}, ignoring"
            )

    # Fall back to config dates if no explicit dates and not skipping
    if not cutoff_start_date and not skip_date_filtering:
        if min_send_date:
            try:
                cutoff_start_date = parse_mailchimp_timestamp(min_send_date)
            except Exception:
                logger.warning(
                    f"Invalid min_send_date format: {min_send_date}, ignoring"
                )

        if not cutoff_start_date and lookback_days:
            cutoff_start_date = datetime.now(timezone.utc) - timedelta(
                days=lookback_days
            )

    if not cutoff_start_date and not cutoff_end_date and not min_emails_sent:
        return campaigns

    logger.info(
        f"filter_campaigns_by_date_and_activity: cutoff_start_date={cutoff_start_date}, cutoff_end_date={cutoff_end_date}, min_emails_sent={min_emails_sent}"
    )

    filtered = []
    for campaign in campaigns:
        send_time_raw = campaign.get("send_time")
        emails_sent = safe_int(campaign.get("emails_sent", 0)) or 0

        if min_emails_sent and emails_sent < min_emails_sent:
            continue

        if (cutoff_start_date or cutoff_end_date) and send_time_raw:
            try:
                send_time = parse_mailchimp_timestamp(send_time_raw)
                if send_time:
                    if cutoff_start_date and send_time < cutoff_start_date:
                        continue
                    if cutoff_end_date and send_time > cutoff_end_date:
                        continue
            except Exception:
                continue

        filtered.append(campaign)

    original_count = len(campaigns)
    filtered_count = len(filtered)
    skipped = original_count - filtered_count

    if skipped > 0:
        logger.info(
            f"Campaign filtering: {filtered_count:,} campaigns selected, {skipped:,} skipped"
        )
        if cutoff_start_date and cutoff_end_date:
            logger.info(
                f"  Filter: {cutoff_start_date.date()} <= send_time <= {cutoff_end_date.date()}"
            )
        elif cutoff_start_date:
            logger.info(f"  Filter: send_time >= {cutoff_start_date.date()}")
        elif cutoff_end_date:
            logger.info(f"  Filter: send_time <= {cutoff_end_date.date()}")
        if min_emails_sent:
            logger.info(f"  Filter: emails_sent >= {min_emails_sent:,}")

    return filtered


def should_process_campaign_for_table(
    campaign: Dict[str, Any], table_name: str
) -> bool:
    emails_sent = safe_int(campaign.get("emails_sent")) or 0
    report_summary = campaign.get("report_summary") or {}
    opens = safe_int(report_summary.get("opens")) or 0
    unique_opens = safe_int(report_summary.get("unique_opens")) or 0
    clicks = safe_int(report_summary.get("clicks")) or 0

    if table_name == "campaign_sent_to":
        return emails_sent > 0
    if table_name == "campaign_open_details":
        return (opens or unique_opens) > 0
    if table_name in {"campaign_click_details", "campaign_click_members"}:
        return clicks > 0
    if table_name == "campaign_email_activity":
        return any(value > 0 for value in (emails_sent, opens, unique_opens, clicks))
    return True


def filter_campaigns_for_table_activity(
    campaigns: List[Dict[str, Any]], table_name: str, logger: logging.Logger
) -> List[Dict[str, Any]]:
    if not campaigns:
        return campaigns
    filtered = [
        campaign
        for campaign in campaigns
        if should_process_campaign_for_table(campaign, table_name)
    ]
    skipped = len(campaigns) - len(filtered)
    if skipped > 0:
        logger.info(
            "Campaign activity filter for %s: %d skipped (no relevant activity)",
            table_name,
            skipped,
        )
    return filtered


def get_campaigns(
    session: requests.Session,
    base_url: str,
    cache: Dict[str, Any],
    cache_key: str,
    since: Optional[str],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    fields: Optional[str] = None,
    progress_logger: Optional[Any] = None,
    progress_interval: int = 10000,
    extra_params: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    campaigns_cache = cache.setdefault("campaigns_cache", {})
    extra_key = tuple(sorted((extra_params or {}).items()))
    cache_token = (
        cache_key,
        since or "all",
        fields or "all",
        page_size,
        extra_key,
        strategy,
    )
    fetch_mode = (cache.get("campaign_fetch_mode") or "api_first").lower()
    preloaded_campaigns = cache.get("preloaded_campaigns")
    use_cache_only = False
    if fetch_mode == "cache_only":
        if preloaded_campaigns:
            use_cache_only = True
        else:
            logger.debug(
                "Campaign cache_only mode requested but no cached campaigns present; falling back to API"
            )
    elif fetch_mode == "prefer_cache" and preloaded_campaigns:
        use_cache_only = True

    if use_cache_only:
        if cache_token in campaigns_cache:
            return campaigns_cache[cache_token]
        campaigns = filter_preloaded_campaigns(preloaded_campaigns, since, extra_params)
        campaigns_cache[cache_token] = campaigns
        return campaigns
    if cache_token in campaigns_cache:
        return campaigns_cache[cache_token]
    params: Dict[str, Any] = {}
    if since:
        params["since_send_time"] = since
    if fields:
        params["fields"] = fields
    if extra_params:
        params.update(extra_params)
    fetcher = (
        batch_fetch_paginated_collection
        if strategy == "batch"
        else fetch_paginated_collection
    )
    try:
        campaigns = fetcher(
            session,
            base_url,
            "/campaigns",
            "campaigns",
            params,
            page_size,
            max_attempts,
            backoff_seconds,
            progress_logger=progress_logger,
            progress_interval=progress_interval,
        )
    except RuntimeError as exc:
        if strategy == "batch":
            logger.warning(
                "Batch campaign fetch failed (%s); falling back to REST", exc
            )
            campaigns = fetch_paginated_collection(
                session,
                base_url,
                "/campaigns",
                "campaigns",
                params,
                page_size,
                max_attempts,
                backoff_seconds,
                progress_logger=progress_logger,
                progress_interval=progress_interval,
            )
        else:
            raise
    campaigns_cache[cache_token] = campaigns
    return campaigns


CAMPAIGN_FIELDS = [
    "campaigns.id",
    "campaigns.web_id",
    "campaigns.parent_campaign_id",
    "campaigns.type",
    "campaigns.create_time",
    "campaigns.archive_url",
    "campaigns.long_archive_url",
    "campaigns.status",
    "campaigns.emails_sent",
    "campaigns.send_time",
    "campaigns.content_type",
    "campaigns.needs_block_refresh",
    "campaigns.resendable",
    "campaigns.recipients",
    "campaigns.settings",
    "campaigns.variate_settings",
    "campaigns.tracking",
    "campaigns.rss_opts",
    "campaigns.ab_split_opts",
    "campaigns.social_card",
    "campaigns.report_summary",
    "campaigns.delivery_status",
    "campaigns._links",
]

LIST_FIELDS = [
    "lists.id",
    "lists.name",
    "lists.contact",
    "lists.permission_reminder",
    "lists.use_archive_bar",
    "lists.campaign_defaults",
    "lists.notify_on_subscribe",
    "lists.notify_on_unsubscribe",
    "lists.date_created",
    "lists.email_type_option",
    "lists.list_rating",
    "lists.stats",
    "lists.modules",
    "lists.subscribe_url_short",
    "lists.subscribe_url_long",
    "lists.visibility",
    "lists.beamer_address",
    "lists._links",
]

MEMBER_FIELDS = [
    "members.id",
    "members.email_address",
    "members.unique_email_id",
    "members.web_id",
    "members.list_id",
    "members.email_type",
    "members.status",
    "members.status_if_new",
    "members.merge_fields",
    "members.stats",
    "members.ip_signup",
    "members.timestamp_signup",
    "members.ip_opt",
    "members.timestamp_opt",
    "members.language",
    "members.location",
    "members.last_changed",
    "members.email_client",
    "members.member_rating",
    "members.marketing_permissions",
    "members.interests",
    "members.vip",
    "members.tags",
    "members.last_note",
    "members._links",
]

from shared.cli_utils import get_bigquery_client
from shared.bigquery_utils import ensure_dataset_exists
from shared.gcs_pipeline import execute_full_pipeline, DEFAULT_TTL_DAYS
from shared.extractor_runner import (
    PipelineEnvironment,
    drop_production_table_if_needed,
    initialize_pipeline_environment,
)

logger = logging.getLogger(__name__)


def get_mailchimp_credentials(config: Dict[str, Any]) -> Tuple[str, str]:
    connection = config.get("source", {}).get("connection", {})
    api_key = connection.get("api_key") or os.getenv("MAILCHIMP_API_KEY")
    server_prefix = connection.get("server_prefix") or os.getenv(
        "MAILCHIMP_SERVER_PREFIX"
    )
    if not api_key:
        raise ValueError("MAILCHIMP_API_KEY not configured")
    if not server_prefix:
        raise ValueError("MAILCHIMP_SERVER_PREFIX not configured")
    return api_key, server_prefix


def fetch_account_metadata(
    session: requests.Session, base_url: str, cache: Dict[str, Any]
) -> Dict[str, Any]:
    if "account_metadata" in cache:
        return cache["account_metadata"]
    try:
        payload = mailchimp_request(session, base_url, "GET", "/", None, 3, 2.0)
    except Exception as exc:
        logger.warning("Unable to fetch Mailchimp account metadata: %s", exc)
        payload = {}
    metadata = {
        "account_id": payload.get("account_id") or "",
        "account_name": payload.get("account_name") or "",
        "email": payload.get("email", ""),
        "total_subscribers": payload.get("total_subscribers"),
    }
    cache["account_metadata"] = metadata
    return metadata


def resolve_mailchimp_accounts(
    config: Dict[str, Any], sites: Optional[List[str]]
) -> List[Dict[str, str]]:
    accounts_cfg = config.get("source", {}).get("accounts") or []
    normalized_sites = []
    if sites and sites != ["all"]:
        normalized_sites = [s for s in sites if s]

    accounts: List[Dict[str, str]] = []

    if accounts_cfg:
        for entry in accounts_cfg:
            name = entry.get("name") or entry.get("label")
            api_key = entry.get("api_key") or entry.get("token")
            server_prefix = entry.get("server_prefix") or entry.get("dc")

            if not api_key or not server_prefix:
                raise ValueError(
                    "Mailchimp account entry requires api_key and server_prefix"
                )

            if normalized_sites and name and name not in normalized_sites:
                continue

            account_name = name or server_prefix
            accounts.append(
                {
                    "name": account_name,
                    "api_key": api_key,
                    "server_prefix": server_prefix,
                    "extra_fields": entry.get("extra_fields", {}),
                }
            )

        if normalized_sites and not accounts:
            raise ValueError(
                f"No Mailchimp accounts matched requested sites: {normalized_sites}"
            )
    else:
        api_key, server_prefix = get_mailchimp_credentials(config)
        if normalized_sites and len(normalized_sites) > 1:
            raise ValueError(
                "Multiple sites requested but no accounts configured in mailchimp.yaml"
            )
        account_name = (
            normalized_sites[0]
            if normalized_sites
            else config.get("source", {}).get("name", "mailchimp")
        )
        accounts.append(
            {
                "name": account_name,
                "api_key": api_key,
                "server_prefix": server_prefix,
                "extra_fields": {},
            }
        )

    return accounts


def get_filtered_sent_campaigns(
    session: requests.Session,
    base_url: str,
    cache: Dict[str, Any],
    cache_key: str,
    since: Optional[str],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    strategy: str = "rest",
) -> tuple[List[Dict[str, Any]], Optional[datetime]]:
    campaign_cache = cache.setdefault("campaign_report_windows", {})
    campaign_filters = cache.get("campaign_filters")
    # Include explicit dates in cache token to avoid returning wrong cached results
    explicit_start = cache.get("explicit_start_date")
    explicit_end = cache.get("explicit_end_date")
    token = (
        cache_key,
        since or "all",
        page_size,
        strategy,
        tuple(sorted((campaign_filters or {}).items())),
        explicit_start,
        explicit_end,
    )
    if token in campaign_cache:
        return campaign_cache[token]

    campaigns = get_campaigns(
        session,
        base_url,
        cache,
        cache_key=cache_key,
        since=since,
        page_size=page_size,
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        fields="campaigns.id,campaigns.recipients,campaigns.send_time,campaigns.status,campaigns.emails_sent,campaigns.report_summary",
        extra_params={"status": "sent"},
        strategy=strategy,
    )

    since_dt = parse_mailchimp_timestamp(since)
    filtered: List[Dict[str, Any]] = []
    for campaign in campaigns:
        send_dt = parse_mailchimp_timestamp(campaign.get("send_time"))
        if since_dt and (send_dt is None or send_dt < since_dt):
            continue
        filtered.append(campaign)

    # Pass explicit dates if provided via command line (they override config)
    skip_date_filtering = since is not None
    explicit_start = cache.get("explicit_start_date") if cache else None
    explicit_end = cache.get("explicit_end_date") if cache else None
    logger.debug(
        "Before filter_campaigns_by_date_and_activity: %d campaigns, explicit_start=%s, explicit_end=%s",
        len(filtered),
        explicit_start,
        explicit_end,
    )
    filtered = filter_campaigns_by_date_and_activity(
        filtered,
        campaign_filters,
        logger,
        skip_date_filtering,
        explicit_start,
        explicit_end,
    )
    logger.debug(
        "After filter_campaigns_by_date_and_activity: %d campaigns", len(filtered)
    )

    campaign_cache[token] = (filtered, since_dt)
    return filtered, since_dt


def normalize_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=serialize_datetime, sort_keys=True)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return value


def serialize_datetime(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def normalize_records(
    records: List[Dict[str, Any]], default_fields: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for original in records:
        row = {}
        for key, value in original.items():
            row[key] = normalize_value(value)
        if default_fields:
            for k, v in default_fields.items():
                row.setdefault(k, v)
        normalized.append(row)
    return normalized


class JsonlWriter:
    def __init__(self, path: str):
        self.path = path
        self.handle = open(path, "w", encoding="utf-8")
        self.count = 0

    def write(self, record: Dict[str, Any]) -> None:
        self.handle.write(json.dumps(record, default=serialize_datetime))
        self.handle.write("\n")
        self.count += 1

    def close(self) -> None:
        if not self.handle.closed:
            self.handle.close()


def _batch_iterate_campaign_endpoint(
    session: requests.Session,
    base_url: str,
    campaigns: List[Dict[str, Any]],
    path_template: str,
    data_key: str,
    params_builder: Callable[[Dict[str, Any]], Dict[str, Any]],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    handler: Callable[
        [Dict[str, Any], List[Dict[str, Any]], int, Dict[str, Any]], None
    ],
    label: Optional[str] = None,
) -> None:
    """Iterate a paginated campaign endpoint using Mailchimp batch API."""

    campaign_queue: deque[Tuple[str, int]] = deque()
    campaign_lookup = {}
    for campaign in campaigns:
        campaign_id = campaign.get("id")
        if not campaign_id:
            continue
        campaign_lookup[campaign_id] = campaign
        campaign_queue.append((campaign_id, 0))

    if not campaign_queue:
        return

    while campaign_queue:
        operations: List[Dict[str, Any]] = []
        op_metadata: Dict[str, Tuple[str, int]] = {}

        while campaign_queue and len(operations) < MAX_BATCH_OPERATIONS:
            campaign_id, offset = campaign_queue.popleft()
            campaign = campaign_lookup.get(campaign_id)
            if not campaign:
                continue

            params = params_builder(campaign) or {}
            params = params.copy()
            params["count"] = page_size
            params["offset"] = offset

            path = path_template.format(campaign_id=campaign_id)
            operation_id = f"{campaign_id}:{offset}"
            operations.append(build_batch_operation("GET", path, params, operation_id))
            op_metadata[operation_id] = (campaign_id, offset)

        if not operations:
            break

        responses = execute_mailchimp_batch(
            session,
            base_url,
            operations,
            max_attempts,
            backoff_seconds,
            label=label,
        )

        for op, response in zip(operations, responses):
            op_id = op["operation_id"]
            campaign_id, offset = op_metadata[op_id]
            campaign = campaign_lookup.get(campaign_id)
            if not campaign:
                continue

            status_code = (
                response.get("status_code") if isinstance(response, dict) else None
            )
            if status_code is not None and not (200 <= status_code < 300):
                body_err = response.get("body") if isinstance(response, dict) else None
                err_detail = (
                    body_err.get("detail") if isinstance(body_err, dict) else body_err
                )
                logger.warning(
                    "Batch op %s returned HTTP %s for %s: %s",
                    op_id,
                    status_code,
                    path_template,
                    err_detail,
                )
                continue

            body = response.get("body") if isinstance(response, dict) else {}
            if not isinstance(body, dict):
                body = {}
            items = body.get(data_key) or []
            if isinstance(items, dict):
                items = [items]

            handler(campaign, items, offset, body)

            total_items = body.get("total_items") if isinstance(body, dict) else None
            next_offset = offset + page_size

            has_more = False
            if isinstance(total_items, int):
                has_more = next_offset < total_items
            else:
                has_more = len(items) == page_size

            if has_more:
                campaign_queue.append((campaign_id, next_offset))


def _write_record(
    writer: JsonlWriter,
    record: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]],
) -> None:
    if default_fields:
        for key, value in default_fields.items():
            record.setdefault(key, value)
    normalized = {k: normalize_value(v) for k, v in record.items()}
    writer.write(normalized)


def batch_extract_campaign_sent_to(
    session: requests.Session,
    base_url: str,
    campaigns: List[Dict[str, Any]],
    default_fields: Optional[Dict[str, Any]],
    tenant_mapping: Dict[str, Any],
    lists_cache: Dict[str, Any],
    since: Optional[str],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    temp_dir: str,
    execution_id: Optional[str] = None,
) -> Tuple[List[str], int]:
    if not campaigns:
        return [], 0

    from shared.parquet_writer import ParquetWriter

    writers: Dict[str, ParquetWriter] = {}
    result_files: List[str] = []
    total_rows = 0

    def get_writer(campaign_id: str) -> ParquetWriter:
        writer = writers.get(campaign_id)
        if writer is None:
            path = os.path.join(temp_dir, f"campaign_sent_to_{campaign_id}.parquet")
            writer = ParquetWriter(path, buffer_size=10000)
            writers[campaign_id] = writer
            result_files.append(path)
        return writer

    since_params = {"since": since} if since else {}
    campaign_lookup = {
        campaign.get("id"): campaign for campaign in campaigns if campaign.get("id")
    }

    def params_builder(campaign: Dict[str, Any]) -> Dict[str, Any]:
        return since_params

    def handler(
        campaign: Dict[str, Any],
        items: List[Dict[str, Any]],
        offset: int,
        body: Dict[str, Any],
    ) -> None:
        nonlocal total_rows
        campaign_id = campaign.get("id")
        if not campaign_id:
            return
        list_id = (campaign.get("recipients") or {}).get("list_id")
        writer = get_writer(campaign_id)
        for entry in items or []:
            record = dict(entry)
            record["campaign_id"] = campaign_id
            record.setdefault("list_id", list_id)
            if "subscriber_hash" not in record and record.get("email_address"):
                record["subscriber_hash"] = derive_email_id(
                    campaign_id, record.get("email_address")
                )
            enrich_record_with_tenant_info(record, tenant_mapping, lists_cache)
            # Apply default fields if provided
            if default_fields:
                for key, value in default_fields.items():
                    record.setdefault(key, value)
            # Add execution metadata (extracted_at, execution_id, source)
            if execution_id:
                set_execution_metadata([record], execution_id, source="mailchimp")
            writer.write(record)
            total_rows += 1

    _batch_iterate_campaign_endpoint(
        session,
        base_url,
        campaigns,
        "/reports/{campaign_id}/sent-to",
        "sent_to",
        params_builder,
        page_size=min(page_size, 1000),
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        handler=handler,
        label="campaign_sent_to",
    )

    for writer in writers.values():
        writer.close()

    return result_files, total_rows


def batch_extract_campaign_email_activity(
    session: requests.Session,
    base_url: str,
    campaigns: List[Dict[str, Any]],
    default_fields: Optional[Dict[str, Any]],
    tenant_mapping: Dict[str, Any],
    lists_cache: Dict[str, Any],
    since: Optional[str],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    temp_dir: str,
    execution_id: Optional[str] = None,
) -> Tuple[List[str], int]:
    if not campaigns:
        return [], 0

    from shared.parquet_writer import ParquetWriter

    writers: Dict[str, ParquetWriter] = {}
    result_files: List[str] = []
    total_rows = 0
    since_dt = parse_mailchimp_timestamp(since)

    def get_writer(campaign_id: str) -> ParquetWriter:
        writer = writers.get(campaign_id)
        if writer is None:
            path = os.path.join(
                temp_dir, f"campaign_email_activity_{campaign_id}.parquet"
            )
            writer = ParquetWriter(path, buffer_size=10000)
            writers[campaign_id] = writer
            result_files.append(path)
        return writer

    def params_builder(campaign: Dict[str, Any]) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if since:
            params["since"] = since
        return params

    def handler(
        campaign: Dict[str, Any],
        items: List[Dict[str, Any]],
        offset: int,
        body: Dict[str, Any],
    ) -> None:
        nonlocal total_rows
        campaign_id = campaign.get("id")
        if not campaign_id:
            return
        list_id = (campaign.get("recipients") or {}).get("list_id")
        writer = get_writer(campaign_id)

        for email_entry in items or []:
            email_id = email_entry.get("email_id")
            email_address = email_entry.get("email_address")
            if not email_id and email_address:
                email_id = derive_email_id(campaign_id, email_address)
            activities = email_entry.get("activity") or []
            for activity_entry in activities:
                action = activity_entry.get("action") or activity_entry.get("type")
                timestamp = activity_entry.get("timestamp") or activity_entry.get(
                    "time"
                )
                activity_dt = parse_mailchimp_timestamp(timestamp)
                if since_dt and activity_dt and activity_dt < since_dt:
                    continue
                if since_dt and activity_dt is None and timestamp:
                    parsed = parse_mailchimp_timestamp(timestamp)
                    if parsed and parsed < since_dt:
                        continue
                    activity_dt = parsed
                if since_dt and activity_dt is None:
                    continue

                record: Dict[str, Any] = {
                    "campaign_id": campaign_id,
                    "list_id": list_id,
                    "email_id": email_id,
                    "email_address": email_address,
                    "action": action,
                    "activity_timestamp": activity_dt or timestamp,
                    "activity_type": activity_entry.get("type"),
                    "ip": activity_entry.get("ip"),
                    "url": activity_entry.get("url"),
                    "device": activity_entry.get("device"),
                    "user_agent": activity_entry.get("user_agent"),
                }

                geo_info = activity_entry.get("geo")
                if isinstance(geo_info, dict):
                    record["geo_country"] = geo_info.get("country")
                    record["geo_region"] = geo_info.get("region")
                else:
                    record["geo_country"] = activity_entry.get("country")
                    record["geo_region"] = activity_entry.get("region")

                if record.get("activity_timestamp") and isinstance(
                    record["activity_timestamp"], datetime
                ):
                    record["activity_timestamp"] = record[
                        "activity_timestamp"
                    ].isoformat()
                elif isinstance(record.get("activity_timestamp"), str):
                    record["activity_timestamp"] = record["activity_timestamp"]

                enrich_record_with_tenant_info(record, tenant_mapping, lists_cache)
                # Apply default fields if provided
                if default_fields:
                    for key, value in default_fields.items():
                        record.setdefault(key, value)
                # Add execution metadata (extracted_at, execution_id, source)
                if execution_id:
                    set_execution_metadata([record], execution_id, source="mailchimp")
                writer.write(record)
                total_rows += 1

    _batch_iterate_campaign_endpoint(
        session,
        base_url,
        campaigns,
        "/reports/{campaign_id}/email-activity",
        "emails",
        params_builder,
        page_size=min(page_size, 1000),
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        handler=handler,
        label="campaign_email_activity",
    )

    for writer in writers.values():
        writer.close()

    return result_files, total_rows


# ---------------------------------------------------------------------------
# Descriptor-driven Batch API extraction (isolation: additive code path,
# reached ONLY when --use-batch-api is passed for campaign_group).
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CampaignBatchTable:
    name: str
    path_template: str
    data_key: str
    paginated: bool = True
    since_param: Optional[str] = "since"
    activity_gate: Optional[str] = None
    record_transform: Optional[Callable[..., List[Dict[str, Any]]]] = None
    op_builder: Optional[Callable[..., List[Dict[str, Any]]]] = None


# API fields each campaign report endpoint returns. Used to build the `fields`
# query param so Mailchimp only serializes columns we actually load; measured
# 3.9x smaller responses on email-activity and 2.6x on sent-to at count=500.
# Intersected with the table's configured schema at request time, so adding a
# schema column automatically starts requesting it -- no list to maintain here.
CAMPAIGN_API_FIELDS: Dict[str, Tuple[str, ...]] = {
    "campaign_sent_to": (
        "absplit_group",
        "campaign_id",
        "email_address",
        "email_id",
        "gmt_offset",
        "last_open",
        "list_id",
        "list_is_active",
        "merge_fields",
        "open_count",
        "status",
        "vip",
    ),
    "campaign_email_activity": (
        "activity",
        "campaign_id",
        "email_address",
        "email_id",
        "list_id",
        "list_is_active",
    ),
    "campaign_open_details": (
        "campaign_id",
        "contact_status",
        "email_address",
        "email_id",
        "list_id",
        "list_is_active",
        "merge_fields",
        "opens",
        "opens_count",
        "proxy_excluded_opens_count",
        "vip",
    ),
    "campaign_click_details": (
        "campaign_id",
        "click_percentage",
        "id",
        "last_click",
        "total_clicks",
        "unique_click_percentage",
        "unique_clicks",
        "url",
    ),
    "campaign_click_members": (
        "campaign_id",
        "clicks",
        "contact_status",
        "email_address",
        "email_id",
        "list_id",
        "list_is_active",
        "merge_fields",
        "url_id",
        "vip",
    ),
    "campaign_domain_performance": (
        "bounces",
        "bounces_pct",
        "clicks",
        "clicks_pct",
        "delivered",
        "domain",
        "emails_pct",
        "emails_sent",
        "opens",
        "opens_pct",
        "unsubs",
        "unsubs_pct",
    ),
    "campaign_locations": (
        "country_code",
        "opens",
        "proxy_excluded_opens",
        "region",
        "region_name",
    ),
}

# _links is Mailchimp HATEOAS navigation (parent/self hrefs + targetSchema URLs)
# repeated verbatim on every row. Nothing downstream reads it, and the only
# per-row-varying part -- the subscriber hash in the self href -- is already
# stored as email_id. It is a large nested field, so excluding it is most of
# the payload win.
CAMPAIGN_FIELD_EXCLUSIONS = frozenset({"_links"})


def build_campaign_fields_param(
    table_name: str, data_key: str, table_config: Optional[Dict[str, Any]]
) -> Optional[str]:
    """Build the Mailchimp `fields` query param for a campaign report endpoint.

    Requests only the API fields that map to a configured schema column, so the
    response carries nothing we would discard on load. Returns None when the
    table has no schema or no known field list, which leaves the request
    unprojected -- the previous behavior.
    """
    api_fields = CAMPAIGN_API_FIELDS.get(table_name)
    if not api_fields:
        return None
    schema = (table_config or {}).get("schema") or {}
    if not schema:
        return None
    keep = [f for f in api_fields if f in schema and f not in CAMPAIGN_FIELD_EXCLUSIONS]
    if not keep:
        return None
    # total_items drives pagination, so it must survive the projection.
    return ",".join(["total_items"] + [f"{data_key}.{f}" for f in keep])


# Projections for the REST path, keyed by table name.
#
# The five large campaign tables are configured extraction_strategy: rest, so
# they never reach batch_extract_campaign_table() and cannot use the
# table_config-driven helper above -- the REST worker
# (_process_campaign_all_tables) is handed no table configs. Rather than
# thread configs through every REST call site, this cache is populated once
# per run from the same resources.<table>.schema the batch path reads.
#
# Unpopulated (the default) means every lookup returns None and requests go
# out unprojected, exactly as before.
_REST_FIELDS_CACHE: Dict[str, Optional[str]] = {}


def prime_rest_fields_cache(table_configs: Optional[Dict[str, Any]]) -> None:
    """Populate the REST `fields` projections from the run's table configs.

    Safe to call repeatedly; later calls overwrite with the same values.
    """
    if not table_configs:
        return
    for table_name, desc in CAMPAIGN_BATCH_TABLES.items():
        cfg = table_configs.get(table_name)
        if not cfg:
            continue
        _REST_FIELDS_CACHE[table_name] = build_campaign_fields_param(
            table_name, desc.data_key, cfg
        )


def rest_fields_param(table_name: str) -> Optional[str]:
    """Return the cached `fields` projection for a REST campaign report call."""
    return _REST_FIELDS_CACHE.get(table_name)


def _with_fields(params: Optional[Dict[str, Any]], table_name: str) -> Dict[str, Any]:
    """Shape a REST request's params for one campaign report table.

    Attaches the `fields` projection, and strips `since` for tables whose
    registry entry sets since_param=None. The REST call sites build their own
    params, so without this the registry's opt-out would apply only to the
    batch path -- and campaign_open_details silently loses ~19% of its members
    when `since` is sent.
    """
    out = dict(params or {})
    desc = CAMPAIGN_BATCH_TABLES.get(table_name)
    if desc is not None and desc.since_param is None:
        out.pop("since", None)
    fields = rest_fields_param(table_name)
    if fields:
        out["fields"] = fields
    return out


def _default_record_transform(
    item: Dict[str, Any],
    campaign: Dict[str, Any],
    list_id: Optional[str],
    table_name: str,
) -> List[Dict[str, Any]]:
    # Produce one row from one API item. Matches REST path shaping for
    # campaign_sent_to / campaign_open_details / campaign_click_details /
    # campaign_click_members / campaign_domain_performance / campaign_locations.
    campaign_id = campaign.get("id")
    row = dict(item)
    row["campaign_id"] = campaign_id
    row.setdefault("list_id", list_id)
    if table_name in (
        "campaign_open_details",
        "campaign_click_members",
        "campaign_sent_to",
    ):
        if not row.get("subscriber_hash") and row.get("email_address"):
            row["subscriber_hash"] = derive_email_id(campaign_id, row["email_address"])
        if not row.get("subscriber_hash"):
            return []
    return [row]


def _explode_email_activity_actions(
    item: Dict[str, Any],
    campaign: Dict[str, Any],
    list_id: Optional[str],
    table_name: str,
    since_dt: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    # Flatten /reports/{id}/email-activity shape — one row per activity entry.
    # Mirrors record shaping at plugins/mailchimp_extractor.py:5772-5807 (REST path)
    # and plugins/mailchimp_extractor.py:1237-1293 (legacy batch path).
    campaign_id = campaign.get("id")
    email_id = item.get("email_id")
    email_address = item.get("email_address")
    if not email_id and email_address:
        email_id = derive_email_id(campaign_id, email_address)
    rows: List[Dict[str, Any]] = []
    for activity_entry in item.get("activity") or []:
        action = activity_entry.get("action") or activity_entry.get("type")
        timestamp = activity_entry.get("timestamp") or activity_entry.get("time")
        activity_dt = parse_mailchimp_timestamp(timestamp) if timestamp else None
        if since_dt and activity_dt and activity_dt < since_dt:
            continue
        if since_dt and activity_dt is None:
            continue
        record: Dict[str, Any] = {
            "campaign_id": campaign_id,
            "list_id": list_id,
            "email_id": email_id,
            "email_address": email_address,
            "action": action,
            "activity_timestamp": (
                activity_dt.isoformat()
                if isinstance(activity_dt, datetime)
                else timestamp
            ),
            "activity_type": activity_entry.get("type"),
            "ip": activity_entry.get("ip"),
            "url": activity_entry.get("url"),
            "device": activity_entry.get("device"),
            "user_agent": activity_entry.get("user_agent"),
        }
        geo_info = activity_entry.get("geo")
        if isinstance(geo_info, dict):
            record["geo_country"] = geo_info.get("country")
            record["geo_region"] = geo_info.get("region")
        else:
            record["geo_country"] = activity_entry.get("country")
            record["geo_region"] = activity_entry.get("region")
        rows.append(record)
    return rows


def _derive_open_details_fields(
    item: Dict[str, Any],
    campaign: Dict[str, Any],
    list_id: Optional[str],
    table_name: str,
) -> List[Dict[str, Any]]:
    # Mailchimp's /open-details returns one record per recipient with a nested
    # `opens` array of {is_proxy_open, timestamp} events but does NOT populate
    # last_open / total_opens / unique_opens at the top level. Derive them
    # from the array so reporting can filter and aggregate on these columns.
    campaign_id = campaign.get("id")
    row = dict(item)
    row["campaign_id"] = campaign_id
    row.setdefault("list_id", list_id)
    if not row.get("subscriber_hash") and row.get("email_address"):
        row["subscriber_hash"] = derive_email_id(campaign_id, row["email_address"])
    if not row.get("subscriber_hash"):
        return []

    opens_arr = item.get("opens") if isinstance(item.get("opens"), list) else None
    if opens_arr:
        timestamps: List[str] = [
            o.get("timestamp")
            for o in opens_arr
            if isinstance(o, dict) and o.get("timestamp")
        ]
        if timestamps and row.get("last_open") is None:
            # Lexicographic max works for ISO-8601 timestamps in UTC.
            row["last_open"] = max(timestamps)
        if row.get("total_opens") is None:
            row["total_opens"] = len(opens_arr)
        if row.get("unique_opens") is None:
            row["unique_opens"] = sum(
                1
                for o in opens_arr
                if isinstance(o, dict) and not o.get("is_proxy_open")
            )
    return [row]


def _derive_click_member_fields(
    item: Dict[str, Any],
    campaign: Dict[str, Any],
    list_id: Optional[str],
    table_name: str,
) -> List[Dict[str, Any]]:
    # Mailchimp's /click-details/{link_id}/members returns a per-member click
    # count under `clicks` but never populates `click_count`. Copy `clicks`
    # into `click_count` so both columns are queryable. `last_click` cannot
    # be derived from this endpoint - it is filled by the post-process SQL
    # that joins to campaign_email_activity.
    campaign_id = campaign.get("id")
    row = dict(item)
    row["campaign_id"] = campaign_id
    row.setdefault("list_id", list_id)
    if not row.get("subscriber_hash") and row.get("email_address"):
        row["subscriber_hash"] = derive_email_id(campaign_id, row["email_address"])
    if not row.get("subscriber_hash"):
        return []
    if row.get("click_count") is None and row.get("clicks") is not None:
        row["click_count"] = row["clicks"]
    return [row]


def _build_click_member_ops(
    campaigns: List[Dict[str, Any]],
    shared_state: Dict[str, Any],
    since: Optional[str],
    page_size: int,
    fields_param: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Tuple[str, str, int]]]:
    # Expand each campaign into (campaign_id, link_id) ops using the link cache
    # populated by campaign_click_details. Prune links with no activity in the
    # since window. Returns (operations, op_metadata) where op_metadata maps
    # operation_id -> (campaign_id, link_id, offset).
    click_urls: Dict[str, List[Dict[str, Any]]] = shared_state.get("click_urls") or {}
    since_dt = parse_mailchimp_timestamp(since) if since else None
    operations: List[Dict[str, Any]] = []
    op_metadata: Dict[str, Tuple[str, str, int]] = {}
    for campaign in campaigns:
        campaign_id = campaign.get("id")
        if not campaign_id:
            continue
        links = click_urls.get(campaign_id) or []
        for link in links:
            link_id = link.get("id")
            if not link_id:
                continue
            unique_clicks = safe_int(link.get("unique_clicks")) or 0
            total_clicks = safe_int(link.get("total_clicks")) or 0
            if unique_clicks == 0 and total_clicks == 0:
                continue
            if since_dt:
                last_click_dt = parse_mailchimp_timestamp(link.get("last_click"))
                if last_click_dt and last_click_dt < since_dt:
                    continue
            params: Dict[str, Any] = {"count": page_size, "offset": 0}
            if since:
                params["since"] = since
            if fields_param:
                params["fields"] = fields_param
            path = f"/reports/{campaign_id}/click-details/{link_id}/members"
            operation_id = f"{campaign_id}:{link_id}:0"
            operations.append(build_batch_operation("GET", path, params, operation_id))
            op_metadata[operation_id] = (campaign_id, link_id, 0)
    return operations, op_metadata


def _batch_single_op_per_campaign(
    session: requests.Session,
    base_url: str,
    campaigns: List[Dict[str, Any]],
    path_template: str,
    data_key: str,
    params_builder: Callable[[Dict[str, Any]], Dict[str, Any]],
    handler: Callable[[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]], None],
    max_attempts: int,
    backoff_seconds: float,
    poll_interval: int = DEFAULT_BATCH_POLL_INTERVAL_SECONDS,
    label: Optional[str] = None,
) -> None:
    # Batch-dispatch a single GET per campaign, chunked by MAX_BATCH_OPERATIONS.
    # Used for non-paginated endpoints: domain-performance, locations.
    campaign_lookup: Dict[str, Dict[str, Any]] = {}
    operations: List[Dict[str, Any]] = []
    op_metadata: Dict[str, str] = {}
    for campaign in campaigns:
        campaign_id = campaign.get("id")
        if not campaign_id:
            continue
        campaign_lookup[campaign_id] = campaign
        params = params_builder(campaign) or {}
        path = path_template.format(campaign_id=campaign_id)
        operation_id = f"{campaign_id}:single"
        operations.append(build_batch_operation("GET", path, params, operation_id))
        op_metadata[operation_id] = campaign_id

    if not operations:
        return

    for idx in range(0, len(operations), MAX_BATCH_OPERATIONS):
        chunk = operations[idx : idx + MAX_BATCH_OPERATIONS]
        responses = execute_mailchimp_batch(
            session,
            base_url,
            chunk,
            max_attempts,
            backoff_seconds,
            poll_interval,
            label=label,
        )
        for op, response in zip(chunk, responses):
            op_id = op["operation_id"]
            campaign_id = op_metadata.get(op_id)
            if not campaign_id:
                continue
            campaign = campaign_lookup.get(campaign_id)
            if not campaign:
                continue
            body = response.get("body") if isinstance(response, dict) else {}
            if not isinstance(body, dict):
                body = {}
            items = body.get(data_key) or []
            if isinstance(items, dict):
                items = [items]
            handler(campaign, items, body)


CAMPAIGN_BATCH_TABLES: Dict[str, CampaignBatchTable] = {
    "campaign_sent_to": CampaignBatchTable(
        name="campaign_sent_to",
        path_template="/reports/{campaign_id}/sent-to",
        data_key="sent_to",
        activity_gate="emails_sent",
    ),
    "campaign_email_activity": CampaignBatchTable(
        name="campaign_email_activity",
        path_template="/reports/{campaign_id}/email-activity",
        data_key="emails",
        activity_gate="emails_sent",
        record_transform=_explode_email_activity_actions,
    ),
    "campaign_open_details": CampaignBatchTable(
        name="campaign_open_details",
        path_template="/reports/{campaign_id}/open-details",
        data_key="members",
        activity_gate="opens",
        record_transform=_derive_open_details_fields,
        # since_param is deliberately None: /open-details filters MEMBERS by
        # their last open, so a 30-day `since` drops every recipient whose most
        # recent open falls outside the window -- taking their entire open
        # history with them. Rows are cumulative per member, not events, so
        # there is no redundancy for `since` to trim.
        #
        # The header hides it: total_items reports the unfiltered count while
        # the paginated body IS filtered. Measured on campaign 18520354b8 --
        # total_items=5187 either way, but paginating yielded 5187 without
        # `since` and 4194 with it. Production held 4199 of 5187 members
        # (19% missing) and 5999 of 7533 opens (20% missing), reproduced across
        # six campaigns at 15-28% loss.
        #
        # campaign_click_members is NOT affected -- /click-details/{id}/members
        # ignores `since` entirely (verified: identical counts with and
        # without), so it keeps its default.
        since_param=None,
    ),
    "campaign_click_details": CampaignBatchTable(
        name="campaign_click_details",
        path_template="/reports/{campaign_id}/click-details",
        data_key="urls_clicked",
        activity_gate="clicks",
    ),
    "campaign_click_members": CampaignBatchTable(
        name="campaign_click_members",
        path_template="/reports/{campaign_id}/click-details/{link_id}/members",
        data_key="members",
        activity_gate="clicks",
        op_builder=_build_click_member_ops,
        record_transform=_derive_click_member_fields,
    ),
    "campaign_domain_performance": CampaignBatchTable(
        name="campaign_domain_performance",
        path_template="/reports/{campaign_id}/domain-performance",
        data_key="domains",
        paginated=False,
        since_param=None,
    ),
    "campaign_locations": CampaignBatchTable(
        name="campaign_locations",
        path_template="/reports/{campaign_id}/locations",
        data_key="locations",
        paginated=False,
        since_param=None,
    ),
}


_CLICK_MEMBER_OP_METADATA_KEY = "__click_member_op_metadata__"


def batch_extract_campaign_table(
    session: requests.Session,
    base_url: str,
    campaigns: List[Dict[str, Any]],
    table_desc: CampaignBatchTable,
    since: Optional[str],
    default_fields: Optional[Dict[str, Any]],
    tenant_mapping: Dict[str, Any],
    lists_cache: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    temp_dir: str,
    shared_state: Dict[str, Any],
    execution_id: Optional[str] = None,
    table_config: Optional[Dict[str, Any]] = None,
) -> Tuple[List[str], int]:
    # Generic descriptor-driven batch extractor. All table-specific behavior
    # flows through table_desc + two optional hooks (record_transform, op_builder).
    # One ParquetWriter per table (inside the per-execution temp_dir).
    if not campaigns:
        return [], 0

    from shared.parquet_writer import ParquetWriter

    gated = [
        c for c in campaigns if should_process_campaign_for_table(c, table_desc.name)
    ]
    if not gated:
        logger.info(
            "[batch] %s: no eligible campaigns after activity gate, skipping",
            table_desc.name,
        )
        return [], 0

    fields_param = build_campaign_fields_param(
        table_desc.name, table_desc.data_key, table_config
    )

    logger.info(
        "[batch] %s: starting batch extraction for %d campaigns (since=%s, projected_fields=%s)",
        table_desc.name,
        len(gated),
        since or "full",
        "yes" if fields_param else "no",
    )

    since_dt = parse_mailchimp_timestamp(since) if since else None
    output_path = os.path.join(
        temp_dir, f"{table_desc.name}_batch_{uuid.uuid4().hex}.parquet"
    )
    writer = ParquetWriter(output_path, buffer_size=10000)
    total_rows = 0
    had_rows = False

    def finalize_record(record: Dict[str, Any]) -> None:
        nonlocal total_rows, had_rows
        enrich_record_with_tenant_info(record, tenant_mapping, lists_cache)
        if default_fields:
            for key, value in default_fields.items():
                record.setdefault(key, value)
        if execution_id:
            set_execution_metadata([record], execution_id, source="mailchimp")
        writer.write(record)
        total_rows += 1
        had_rows = True

    def shape_rows(
        campaign: Dict[str, Any], item: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        list_id = (campaign.get("recipients") or {}).get("list_id")
        if table_desc.record_transform is not None:
            if table_desc.record_transform is _explode_email_activity_actions:
                return _explode_email_activity_actions(
                    item, campaign, list_id, table_desc.name, since_dt=since_dt
                )
            return table_desc.record_transform(item, campaign, list_id, table_desc.name)
        return _default_record_transform(item, campaign, list_id, table_desc.name)

    try:
        if table_desc.op_builder is not None:
            # Custom op shape (currently only campaign_click_members).
            # The op_builder seeds the work queue at offset=0 for each
            # (campaign_id, link_id). After each chunk we inspect total_items
            # and enqueue continuation offsets so heavily-clicked links
            # (>page_size members) are not silently truncated.
            seed_ops, seed_metadata = table_desc.op_builder(
                gated, shared_state, since, page_size, fields_param
            )
            if not seed_ops:
                writer.close()
                if not had_rows:
                    try:
                        os.remove(output_path)
                    except OSError:
                        pass
                    return [], 0
                return [output_path], total_rows

            campaign_lookup = {c.get("id"): c for c in gated if c.get("id")}

            # Work queue of (campaign_id, link_id, offset) tuples. Each chunk
            # below pulls up to MAX_BATCH_OPERATIONS items off the queue,
            # builds ops, executes the batch, and re-enqueues continuations.
            work_queue: deque[Tuple[str, str, int]] = deque()
            for op_id in (op["operation_id"] for op in seed_ops):
                meta = seed_metadata.get(op_id)
                if meta:
                    work_queue.append(meta)

            while work_queue:
                chunk_meta: List[Tuple[str, str, int]] = []
                chunk_ops: List[Dict[str, Any]] = []
                while work_queue and len(chunk_ops) < MAX_BATCH_OPERATIONS:
                    campaign_id, link_id, offset = work_queue.popleft()
                    params: Dict[str, Any] = {"count": page_size, "offset": offset}
                    if since:
                        params["since"] = since
                    if fields_param:
                        params["fields"] = fields_param
                    path = f"/reports/{campaign_id}/click-details/{link_id}/members"
                    op_id = f"{campaign_id}:{link_id}:{offset}"
                    chunk_ops.append(build_batch_operation("GET", path, params, op_id))
                    chunk_meta.append((campaign_id, link_id, offset))

                if not chunk_ops:
                    break

                responses = execute_mailchimp_batch(
                    session,
                    base_url,
                    chunk_ops,
                    max_attempts,
                    backoff_seconds,
                    label="campaign_click_members",
                )
                for (campaign_id, link_id, offset), response in zip(
                    chunk_meta, responses
                ):
                    campaign = campaign_lookup.get(campaign_id)
                    if not campaign:
                        continue
                    body = response.get("body") if isinstance(response, dict) else {}
                    if not isinstance(body, dict):
                        logger.warning(
                            "Batch response for click_members had unparseable body: campaign=%s link=%s offset=%d",
                            campaign_id,
                            link_id,
                            offset,
                        )
                        body = {}
                    items = body.get(table_desc.data_key) or []
                    if isinstance(items, dict):
                        items = [items]
                    for item in items:
                        item_with_link = dict(item)
                        item_with_link["link_id"] = link_id
                        for row in shape_rows(campaign, item_with_link):
                            finalize_record(row)

                    # Re-enqueue continuation if the link has more members.
                    total_items = body.get("total_items")
                    next_offset = offset + page_size
                    if isinstance(total_items, int):
                        if next_offset < total_items:
                            work_queue.append((campaign_id, link_id, next_offset))
                    elif len(items) == page_size:
                        work_queue.append((campaign_id, link_id, next_offset))
        elif table_desc.paginated:

            def params_builder(campaign: Dict[str, Any]) -> Dict[str, Any]:
                params: Dict[str, Any] = {}
                if since and table_desc.since_param:
                    params[table_desc.since_param] = since
                if fields_param:
                    params["fields"] = fields_param
                return params

            def handler(
                campaign: Dict[str, Any],
                items: List[Dict[str, Any]],
                offset: int,
                body: Dict[str, Any],
            ) -> None:
                # Side effect: for click_details, stash per-campaign link list for
                # downstream click_members op building.
                if table_desc.name == "campaign_click_details":
                    campaign_id = campaign.get("id")
                    if campaign_id:
                        cache = shared_state.setdefault("click_urls", {})
                        cache.setdefault(campaign_id, []).extend(items or [])
                for item in items or []:
                    for row in shape_rows(campaign, item):
                        finalize_record(row)

            _batch_iterate_campaign_endpoint(
                session,
                base_url,
                gated,
                table_desc.path_template,
                table_desc.data_key,
                params_builder,
                page_size=min(page_size, 1000),
                max_attempts=max_attempts,
                backoff_seconds=backoff_seconds,
                handler=handler,
                label=table_desc.name,
            )
        else:

            def single_params_builder(campaign: Dict[str, Any]) -> Dict[str, Any]:
                params: Dict[str, Any] = {}
                if since and table_desc.since_param:
                    params[table_desc.since_param] = since
                if fields_param:
                    params["fields"] = fields_param
                return params

            def single_handler(
                campaign: Dict[str, Any],
                items: List[Dict[str, Any]],
                body: Dict[str, Any],
            ) -> None:
                for item in items or []:
                    for row in shape_rows(campaign, item):
                        finalize_record(row)

            _batch_single_op_per_campaign(
                session,
                base_url,
                gated,
                table_desc.path_template,
                table_desc.data_key,
                single_params_builder,
                single_handler,
                max_attempts,
                backoff_seconds,
                label=table_desc.name,
            )
    finally:
        writer.close()

    if not had_rows:
        try:
            os.remove(output_path)
        except OSError:
            pass
        logger.info("[batch] %s: completed, 0 rows written", table_desc.name)
        return [], 0

    logger.info(
        "[batch] %s: completed, %d rows written to %s",
        table_desc.name,
        total_rows,
        output_path,
    )
    return [output_path], total_rows


def _order_for_dependencies(
    new_batch_tables: List[str],
    requested_tables: List[str],
) -> List[str]:
    # Ensure campaign_click_details runs before campaign_click_members so the
    # link cache in shared_state is populated. All other orders are stable.
    ordered: List[str] = []
    requested_set = set(requested_tables)
    priority = {"campaign_click_details": 0, "campaign_click_members": 1}
    candidates = [t for t in new_batch_tables if t in requested_set]
    candidates.sort(key=lambda t: (priority.get(t, 2), t))
    ordered.extend(candidates)
    return ordered


def _partition_campaign_tables_by_strategy(
    requested_tables: List[str],
    table_strategies: Optional[Dict[str, str]],
    use_batch_api: bool,
) -> Tuple[List[str], List[str]]:
    """Split campaign tables into Batch API and REST work lists."""
    strategies = table_strategies or {}
    batch_tables: List[str] = []
    rest_tables: List[str] = []

    for table in requested_tables:
        strategy = (strategies.get(table) or "rest").lower()
        if use_batch_api and strategy == "batch":
            batch_tables.append(table)
        else:
            rest_tables.append(table)

    return batch_tables, rest_tables


def batch_extract_campaign_tables(
    session: requests.Session,
    base_url: str,
    campaigns: List[Dict[str, Any]],
    table_configs: Dict[str, Dict[str, Any]],
    requested_tables: List[str],
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    default_fields: Optional[Dict[str, Any]],
    tenant_mapping: Dict[str, Any],
    lists_cache: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    temp_dir: str,
    execution_id: Optional[str] = None,
    max_workers: int = 1,
) -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}
    results_lock = threading.Lock()

    def _run_sent_to():
        sent_config = table_configs.get("campaign_sent_to", {})
        sent_since = compute_since_timestamp(
            refresh_mode, lookback_days, start_date, sent_config
        )
        files, row_count = batch_extract_campaign_sent_to(
            session,
            base_url,
            campaigns,
            default_fields,
            tenant_mapping,
            lists_cache,
            sent_since,
            page_size,
            max_attempts,
            backoff_seconds,
            temp_dir,
            execution_id=execution_id,
        )
        return "campaign_sent_to", files, row_count

    def _run_email_activity():
        activity_config = table_configs.get("campaign_email_activity", {})
        activity_since = compute_since_timestamp(
            refresh_mode, lookback_days, start_date, activity_config
        )
        files, row_count = batch_extract_campaign_email_activity(
            session,
            base_url,
            campaigns,
            default_fields,
            tenant_mapping,
            lists_cache,
            activity_since,
            page_size,
            max_attempts,
            backoff_seconds,
            temp_dir,
            execution_id=execution_id,
        )
        return "campaign_email_activity", files, row_count

    def _run_descriptor_table(table_name: str, shared_state: Dict[str, Any]):
        desc = CAMPAIGN_BATCH_TABLES.get(table_name)
        if desc is None:
            return table_name, [], 0
        table_cfg = table_configs.get(table_name, {})
        table_since = compute_since_timestamp(
            refresh_mode, lookback_days, start_date, table_cfg
        )
        files, row_count = batch_extract_campaign_table(
            session,
            base_url,
            campaigns,
            desc,
            table_since,
            default_fields,
            tenant_mapping,
            lists_cache,
            page_size,
            max_attempts,
            backoff_seconds,
            temp_dir,
            shared_state,
            execution_id=execution_id,
            table_config=table_cfg,
        )
        return table_name, files, row_count

    # Phase 1: independent tables that can run in parallel.
    # campaign_click_members depends on campaign_click_details (needs shared_state['click_urls'])
    # so it must run AFTER phase 1 completes.
    shared_state: Dict[str, Any] = {"click_urls": {}}
    parallel_jobs = []

    if "campaign_sent_to" in requested_tables:
        parallel_jobs.append(_run_sent_to)
    if "campaign_email_activity" in requested_tables:
        parallel_jobs.append(_run_email_activity)
    for table_name in (
        "campaign_open_details",
        "campaign_click_details",
        "campaign_domain_performance",
        "campaign_locations",
    ):
        if table_name in requested_tables:
            parallel_jobs.append(
                lambda tn=table_name: _run_descriptor_table(tn, shared_state)
            )

    if parallel_jobs:
        effective_workers = max(1, min(max_workers, len(parallel_jobs)))
        logger.info(
            f"[parallel] Starting {len(parallel_jobs)} table extractions (workers={effective_workers})"
        )
        with ThreadPoolExecutor(
            max_workers=effective_workers, thread_name_prefix="mailchimp-table"
        ) as executor:
            futures = [executor.submit(job) for job in parallel_jobs]
            for fut in as_completed(futures):
                table_name, files, row_count = fut.result()
                with results_lock:
                    results[table_name] = {"files": files, "row_count": row_count}
                logger.info(f"[parallel] {table_name} completed: {row_count} rows")

    # Phase 2: campaign_click_members runs after click_details has populated shared_state.
    if "campaign_click_members" in requested_tables:
        table_name, files, row_count = _run_descriptor_table(
            "campaign_click_members", shared_state
        )
        results[table_name] = {"files": files, "row_count": row_count}

    return results


def apply_merge_field_overrides(
    record: Dict[str, Any], mappings: Dict[str, List[str]]
) -> None:
    merge_fields = record.get("merge_fields")
    if not isinstance(merge_fields, dict):
        return
    for target, source_keys in mappings.items():
        if record.get(target):
            continue
        for merge_key in source_keys:
            value = merge_fields.get(merge_key)
            if value not in (None, ""):
                record[target] = value
                break


def compute_since_timestamp(
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
) -> Optional[str]:
    if start_date:
        return f"{start_date}T00:00:00Z"
    if refresh_mode == "full":
        return None
    days = (
        lookback_days or table_config.get("incremental", {}).get("lookback_days") or 30
    )
    since_dt = datetime.now(timezone.utc) - timedelta(days=days)
    return since_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_first_value(row: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def derive_email_id(campaign_id: Optional[str], email_address: Optional[str]) -> str:
    base = f"{(campaign_id or '').lower()}|{(email_address or '').lower()}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def create_account_export_job(
    session: requests.Session,
    base_url: str,
    include_stages: List[str],
    since_timestamp: Optional[str],
    max_attempts: int,
    backoff_seconds: float,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"include_stages": include_stages}
    if since_timestamp:
        payload["since_timestamp"] = since_timestamp
    url = f"{base_url}/account-exports"
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        response = session.post(url, json=payload, timeout=session.timeout)
        if response.status_code in (200, 201, 202):
            try:
                return response.json()
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"Invalid JSON response when creating account export: {exc}"
                )
        if response.status_code in (429,) or response.status_code >= 500:
            if attempt < max_attempts:
                delay = backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "Account export request throttled (%s). Retrying in %.1fs",
                    response.status_code,
                    delay,
                )
                time.sleep(delay)
                continue
        try:
            error_payload = response.json()
        except Exception:
            error_payload = response.text
        if response.status_code == 400 and isinstance(error_payload, dict):
            title = (error_payload.get("title") or "").lower()
            detail = (error_payload.get("detail") or "").lower()
            if (
                "account export in progress" in title
                or "account export in progress" in detail
            ):
                export_id = error_payload.get("export_id") or error_payload.get(
                    "instance"
                )
                raise AccountExportInProgressError(export_id, error_payload)
        raise RuntimeError(
            f"Account export request failed ({response.status_code}): {error_payload}"
        )
    raise RuntimeError("Exceeded retries when creating account export")


def poll_account_export_job(
    session: requests.Session,
    base_url: str,
    export_id: str,
    poll_interval: int = DEFAULT_BULK_POLL_INTERVAL_SECONDS,
    timeout_seconds: int = DEFAULT_BULK_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    url = f"{base_url}/account-exports/{export_id}"
    start_time = time.time()
    while True:
        response = session.get(url, timeout=session.timeout)
        if response.status_code >= 400:
            try:
                error_payload = response.json()
            except Exception:
                error_payload = response.text
            raise RuntimeError(
                f"Failed polling account export {export_id}: {error_payload}"
            )
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Invalid JSON while polling account export {export_id}: {exc}"
            )
        status = (payload.get("status") or "").lower()
        if status == "finished" and payload.get("download_url"):
            return payload
        if status in {"failed", "cancelled", "canceled"}:
            raise RuntimeError(
                f"Account export {export_id} finished with status {status}"
            )
        if time.time() - start_time > timeout_seconds:
            raise RuntimeError(
                f"Timed out waiting for account export {export_id} to finish"
            )
        time.sleep(poll_interval)


def download_account_export_zip(download_url: str, timeout: int = 600) -> str:
    response = requests.get(download_url, stream=True, timeout=timeout)
    if response.status_code >= 400:
        raise RuntimeError(
            f"Failed to download account export: HTTP {response.status_code}"
        )
    fd, temp_path = tempfile.mkstemp(suffix=".zip")
    try:
        with os.fdopen(fd, "wb") as handle:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    handle.write(chunk)
    except Exception:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise
    return temp_path


def fetch_latest_account_export(
    session: requests.Session, base_url: str
) -> Optional[Dict[str, Any]]:
    url = f"{base_url}/account-exports"
    try:
        response = session.get(url, timeout=session.timeout)
    except RequestException as exc:
        logger.debug("Failed to list account exports: %s", exc)
        return None
    if response.status_code >= 400:
        logger.debug("Listing account exports returned HTTP %s", response.status_code)
        return None
    try:
        payload = response.json()
    except json.JSONDecodeError:
        return None

    exports: Optional[List[Dict[str, Any]]] = None
    if isinstance(payload, dict):
        for key in ("account_exports", "exports", "items", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                exports = value
                break
        if exports is None and isinstance(payload.get("results"), list):
            exports = payload["results"]
    elif isinstance(payload, list):
        exports = payload

    if not exports:
        return None

    for job in exports:
        status = (job.get("status") or "").lower()
        # Prefer active jobs first
        if status not in {"finished", "failed", "cancelled", "canceled"}:
            return job

    # Fall back to the most recent finished job with a download URL
    for job in exports:
        status = (job.get("status") or "").lower()
        if status == "finished" and job.get("download_url"):
            return job

    return None


def wait_for_existing_account_export(
    session: requests.Session,
    base_url: str,
    export_id: Optional[str],
    poll_interval: int = DEFAULT_BULK_POLL_INTERVAL_SECONDS,
    timeout_seconds: int = DEFAULT_BULK_TIMEOUT_SECONDS,
) -> Optional[Dict[str, Any]]:
    effective_timeout = min(timeout_seconds, DEFAULT_BULK_EXISTING_WAIT_SECONDS)
    deadline = time.time() + effective_timeout

    def poll_existing(target_id: Any) -> Optional[Dict[str, Any]]:
        remaining = max(int(deadline - time.time()), poll_interval)
        if remaining <= 0:
            return None
        try:
            return poll_account_export_job(
                session,
                base_url,
                str(target_id),
                poll_interval=poll_interval,
                timeout_seconds=remaining,
            )
        except RuntimeError as exc:
            message = str(exc)
            if "Resource Not Found" in message or "404" in message:
                logger.info(
                    "Existing Mailchimp account export %s no longer available (HTTP 404)",
                    target_id,
                )
                return None
            logger.warning(
                "Polling existing Mailchimp account export %s failed: %s",
                target_id,
                exc,
            )
            return None

    if export_id:
        logger.info(
            "Waiting for existing Mailchimp account export %s to finish", export_id
        )
        result = poll_existing(export_id)
        if result:
            return result

    while time.time() < deadline:
        job = fetch_latest_account_export(session, base_url)
        if not job:
            time.sleep(min(poll_interval, 15))
            continue

        active_export_id = job.get("id") or job.get("export_id") or job.get("instance")
        status = (job.get("status") or "").lower()
        if status in {"failed", "cancelled", "canceled"}:
            logger.warning(
                "Existing Mailchimp account export %s ended with status %s",
                active_export_id,
                status,
            )
            return None
        if status == "finished" and job.get("download_url"):
            logger.info(
                "Reusing completed Mailchimp account export %s", active_export_id
            )
            return job
        if active_export_id:
            logger.info(
                "Existing Mailchimp account export %s currently %s; waiting to reuse",
                active_export_id,
                status or "pending",
            )
            result = poll_existing(active_export_id)
            if result:
                return result

        time.sleep(min(poll_interval, 15))

    logger.warning(
        "Timed out waiting for existing Mailchimp account export to finish after %.0fs",
        effective_timeout,
    )
    return None


def request_account_export_job(
    session: requests.Session,
    base_url: str,
    include_stages: List[str],
    since_timestamp: Optional[str],
    max_attempts: int,
    backoff_seconds: float,
) -> Dict[str, Any]:
    wait_attempts = 0

    while True:
        try:
            return create_account_export_job(
                session,
                base_url,
                include_stages,
                since_timestamp,
                max_attempts,
                backoff_seconds,
            )
        except AccountExportInProgressError as in_progress:
            wait_attempts += 1
            logger.info(
                "Existing Mailchimp account export detected; waiting before retry (attempt %d)",
                wait_attempts,
            )
            existing_job = wait_for_existing_account_export(
                session,
                base_url,
                in_progress.export_id,
                poll_interval=DEFAULT_BULK_POLL_INTERVAL_SECONDS,
                timeout_seconds=DEFAULT_BULK_EXISTING_WAIT_SECONDS,
            )
            if existing_job:
                return existing_job
            if wait_attempts >= max_attempts:
                raise RuntimeError(
                    f"Existing Mailchimp account export did not finish after waiting: {in_progress.payload}"
                )
            delay = backoff_seconds * wait_attempts
            logger.info("Retrying Mailchimp account export creation in %.1fs", delay)
            time.sleep(delay)


def build_bulk_unsubscribe_record(
    row: Dict[str, Any], since_dt: Optional[datetime]
) -> Optional[Dict[str, Any]]:
    timestamp_str = get_first_value(
        row, ["timestamp", "time", "event_time", "Event Time", "Time", "Timestamp"]
    )
    unsub_dt = parse_mailchimp_timestamp(timestamp_str)
    if since_dt and unsub_dt and unsub_dt < since_dt:
        return None
    if since_dt and unsub_dt is None and timestamp_str:
        # Attempt fallback parse using centralized utility
        try:
            unsub_dt = parse_mailchimp_timestamp(timestamp_str)
            if unsub_dt and unsub_dt < since_dt:
                return None
        except Exception:
            return None

    email_address = get_first_value(
        row, ["email_address", "email", "Email Address", "Email"]
    )
    if not email_address:
        return None
    campaign_id = (
        get_first_value(row, ["campaign_id", "campaign", "Campaign ID", "Campaign"])
        or ""
    )
    email_id = get_first_value(row, ["email_id", "Email ID", "emailID"])
    if not email_id:
        email_id = derive_email_id(campaign_id, email_address)
    list_id = (
        get_first_value(row, ["list_id", "List ID", "audience_id", "Audience ID"]) or ""
    )
    reason = (
        get_first_value(
            row, ["reason", "Reason", "details", "Details", "unsubscribe_reason"]
        )
        or ""
    )
    first_name = get_first_value(row, ["first_name", "FNAME", "First Name"]) or ""
    last_name = get_first_value(row, ["last_name", "LNAME", "Last Name"]) or ""
    city = get_first_value(row, ["city", "City"]) or ""
    state = get_first_value(row, ["state", "State", "region", "Region"]) or ""
    country = get_first_value(row, ["country", "Country"]) or ""
    subscriber_type = get_first_value(row, ["subscriber_type", "Subscriber Type"]) or ""
    merge_fields = get_first_value(row, ["merge_fields", "Merge Fields"]) or ""
    record_timestamp = unsub_dt.isoformat() if unsub_dt else timestamp_str

    return {
        "campaign_id": campaign_id,
        "email_id": email_id,
        "email_address": email_address,
        "list_id": list_id,
        "reason": reason,
        "timestamp": record_timestamp,
        "merge_fields": merge_fields,
        "first_name": first_name,
        "last_name": last_name,
        "city": city,
        "state": state,
        "country": country,
        "subscriber_type": subscriber_type,
    }


def build_bulk_bounce_record(
    row: Dict[str, Any], since_dt: Optional[datetime]
) -> Optional[Dict[str, Any]]:
    timestamp_str = get_first_value(
        row, ["timestamp", "time", "event_time", "Event Time", "Time", "Timestamp"]
    )
    bounce_dt = parse_mailchimp_timestamp(timestamp_str)
    if since_dt and bounce_dt and bounce_dt < since_dt:
        return None
    if since_dt and bounce_dt is None and timestamp_str:
        try:
            bounce_dt = parse_mailchimp_timestamp(timestamp_str)
            if bounce_dt and bounce_dt < since_dt:
                return None
        except Exception:
            return None

    email_address = get_first_value(
        row, ["email_address", "email", "Email Address", "Email"]
    )
    if not email_address:
        return None
    campaign_id = (
        get_first_value(row, ["campaign_id", "campaign", "Campaign ID", "Campaign"])
        or ""
    )
    email_id = get_first_value(row, ["email_id", "Email ID", "emailID"])
    if not email_id:
        email_id = derive_email_id(campaign_id, email_address)
    list_id = (
        get_first_value(row, ["list_id", "List ID", "audience_id", "Audience ID"]) or ""
    )
    bounce_type = (
        get_first_value(
            row, ["bounce_type", "Bounce Type", "type", "Type", "status", "Status"]
        )
        or ""
    )
    ip_address = get_first_value(row, ["ip", "IP", "ip_address", "IP Address"]) or ""
    url = get_first_value(row, ["url", "URL", "bounce_url"]) or ""
    record_timestamp = bounce_dt.isoformat() if bounce_dt else timestamp_str

    return {
        "campaign_id": campaign_id,
        "email_id": email_id,
        "email_address": email_address,
        "list_id": list_id,
        "bounce_type": bounce_type,
        "bounce_timestamp": record_timestamp,
        "ip": ip_address,
        "url": url,
    }


def parse_account_export_reports(
    zip_path: str,
    target_tables: List[str],
    table_since_map: Dict[str, Optional[datetime]],
) -> Dict[str, List[Dict[str, Any]]]:
    results: Dict[str, List[Dict[str, Any]]] = {table: [] for table in target_tables}
    action_keys = ["action", "Action", "activity", "Activity", "event", "Event"]
    with zipfile.ZipFile(zip_path, "r") as archive:
        for member in archive.namelist():
            lower_name = member.lower()
            if not lower_name.endswith(".csv"):
                continue
            try:
                with archive.open(member) as csv_file:
                    text_stream = io.TextIOWrapper(csv_file, encoding="utf-8-sig")
                    reader = csv.DictReader(text_stream)
                    for row in reader:
                        action_value = (get_first_value(row, action_keys) or "").lower()
                        if "unsubscribes" in target_tables:
                            should_process_unsub = (
                                "unsub" in action_value or "unsubscribe" in lower_name
                            )
                            if should_process_unsub:
                                record = build_bulk_unsubscribe_record(
                                    row, table_since_map.get("unsubscribes")
                                )
                                if record:
                                    results["unsubscribes"].append(record)
                        if "bounces" in target_tables:
                            bounce_action = any(
                                token in action_value
                                for token in ["bounce", "hard bounce", "soft bounce"]
                            )
                            should_process_bounce = (
                                bounce_action or "bounce" in lower_name
                            )
                            if should_process_bounce:
                                record = build_bulk_bounce_record(
                                    row, table_since_map.get("bounces")
                                )
                                if record:
                                    results["bounces"].append(record)
            except KeyError:
                continue
    return results


def extract_bulk_events_for_account(
    session: requests.Session,
    base_url: str,
    table_configs: Dict[str, Dict[str, Any]],
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    max_attempts: int,
    backoff_seconds: float,
) -> Dict[str, List[Dict[str, Any]]]:
    if not table_configs:
        return {}

    table_since_map: Dict[str, Optional[datetime]] = {}
    global_since_dt: Optional[datetime] = None

    for table_name, cfg in table_configs.items():
        since_str = compute_since_timestamp(
            refresh_mode, lookback_days, start_date, cfg
        )
        since_dt = parse_mailchimp_timestamp(since_str)
        table_since_map[table_name] = since_dt
        if since_dt and (global_since_dt is None or since_dt < global_since_dt):
            global_since_dt = since_dt

    since_timestamp_param: Optional[str] = None
    if global_since_dt:
        since_timestamp_param = (
            global_since_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        )

    include_stages = ["reports"]
    logger.info(
        "Requesting Mailchimp account export (stages=%s, since=%s)",
        include_stages,
        since_timestamp_param,
    )
    export_job = request_account_export_job(
        session,
        base_url,
        include_stages,
        since_timestamp_param,
        max_attempts,
        backoff_seconds,
    )
    export_id = export_job.get("id") or export_job.get("export_id")
    if not export_id:
        raise RuntimeError("Account export did not return an export ID")

    if not export_job.get("download_url"):
        export_job = poll_account_export_job(session, base_url, export_id)

    download_url = export_job.get("download_url")
    if not download_url:
        raise RuntimeError("Account export finished without providing download URL")

    zip_path = download_account_export_zip(download_url)
    try:
        parsed_results = parse_account_export_reports(
            zip_path, list(table_configs.keys()), table_since_map
        )
    finally:
        try:
            os.remove(zip_path)
        except OSError:
            pass

    for table_name, records in parsed_results.items():
        logger.info("Account export returned %d %s rows", len(records), table_name)

    return parsed_results


def extract_campaigns(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    campaign_fields = table_config.get("fields") or CAMPAIGN_FIELDS
    fields_param = ",".join(sorted(set(campaign_fields))) if campaign_fields else None
    since = compute_since_timestamp(
        refresh_mode, lookback_days, start_date, table_config
    )
    # Support api_filters from YAML config (e.g., status: sent)
    api_filters = table_config.get("api_filters")
    campaigns = get_campaigns(
        session,
        base_url,
        cache,
        cache_key="campaigns_full",
        since=since,
        page_size=page_size,
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        fields=fields_param,
        progress_logger=lambda count, total: logger.debug(
            "Fetched %d/%s campaign records", count, total if total else "unknown"
        ),
        progress_interval=10000,
        extra_params=api_filters,
        strategy=strategy,
    )
    campaign_filters = cache.get("campaign_filters")
    # Skip date filtering from config if explicit dates were provided via command line
    skip_date_filtering = since is not None
    explicit_start = cache.get("explicit_start_date")
    explicit_end = cache.get("explicit_end_date")
    campaigns = filter_campaigns_by_date_and_activity(
        campaigns,
        campaign_filters,
        logger,
        skip_date_filtering,
        explicit_start,
        explicit_end,
    )
    # Enrich with full /reports/{id} data (bounces, proxy-excluded opens, unsubs, etc.)
    if table_config.get("enrich_reports", True) and campaigns:
        _enrich_campaigns_with_reports(
            campaigns, session, base_url, max_attempts, backoff_seconds, cache
        )
    cache["campaigns_raw_records"] = campaigns
    return normalize_records(campaigns, default_fields)


def extract_lists(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    list_fetch_mode = (cache.get("list_fetch_mode") or "api_first").lower()
    preloaded_lists = cache.get("preloaded_lists")
    lists: Optional[List[Dict[str, Any]]] = None
    if list_fetch_mode in {"cache_only", "prefer_cache"} and preloaded_lists:
        lists = preloaded_lists
    # Do NOT use cache['lists_raw'] here — it contains preloaded metadata
    # from BigQuery (for name lookups), not fresh API data for extraction.
    if lists is None:
        params: Dict[str, Any] = {}
        list_fields = table_config.get("fields") or LIST_FIELDS
        if list_fields:
            params["fields"] = ",".join(sorted(set(list_fields)))
        since = compute_since_timestamp(
            refresh_mode, lookback_days, start_date, table_config
        )
        if since:
            params["since_last_changed"] = since
        fetcher = (
            batch_fetch_paginated_collection
            if strategy == "batch"
            else fetch_paginated_collection
        )
        try:
            lists = fetcher(
                session,
                base_url,
                "/lists",
                "lists",
                params,
                page_size,
                max_attempts,
                backoff_seconds,
            )
        except RuntimeError as exc:
            if strategy == "batch":
                logger.warning(
                    "Batch list fetch failed (%s); falling back to REST", exc
                )
                lists = fetch_paginated_collection(
                    session,
                    base_url,
                    "/lists",
                    "lists",
                    params,
                    page_size,
                    max_attempts,
                    backoff_seconds,
                )
            else:
                raise
        cache["lists_raw"] = lists
        logger.debug("Fetched %d lists from Mailchimp", len(lists))
    elif list_fetch_mode == "cache_only" and not preloaded_lists:
        logger.debug(
            "List cache_only mode requested but no cached lists present; used fallback data"
        )
    if preloaded_lists is not None and lists is not preloaded_lists:
        cache["preloaded_lists"] = preloaded_lists
    cache["lists_raw_records"] = lists
    return normalize_records(lists, default_fields)


def extract_members(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    if "lists_raw" in cache:
        lists = cache["lists_raw"]
    else:
        lists = fetch_paginated_collection(
            session,
            base_url,
            "/lists",
            "lists",
            {},
            page_size,
            max_attempts,
            backoff_seconds,
        )
        cache["lists_raw"] = lists
    allowed_lists = table_config.get("list_ids") or [
        lst.get("id") for lst in lists if lst.get("id")
    ]
    params: Dict[str, Any] = {
        "status": table_config.get("member_status", "subscribed,cleaned")
    }
    since = compute_since_timestamp(
        refresh_mode, lookback_days, start_date, table_config
    )
    if since:
        params["since_last_changed"] = since
    member_fields = table_config.get("fields") or MEMBER_FIELDS
    if member_fields:
        params["fields"] = ",".join(sorted(set(member_fields)))
    members: List[Dict[str, Any]] = []
    failed_lists: List[str] = []
    use_batch = strategy == "batch"
    for list_id in allowed_lists:
        if not list_id:
            continue
        path = f"/lists/{list_id}/members"
        if use_batch:
            try:
                records = batch_fetch_paginated_collection(
                    session,
                    base_url,
                    path,
                    "members",
                    params,
                    page_size,
                    max_attempts,
                    backoff_seconds,
                    progress_logger=lambda count, total, lid=list_id: logger.debug(
                        "Fetched %d/%s members for list %s",
                        count,
                        total if total else "unknown",
                        lid,
                    ),
                    progress_interval=10000,
                )
                logger.debug(
                    "Batch fetched %d members for list %s", len(records), list_id
                )
                for record in records:
                    record["list_id"] = list_id
                members.extend(records)
                continue
            except RuntimeError as exc:
                logger.warning(
                    "Batch member fetch failed for list %s: %s; falling back to REST",
                    list_id,
                    exc,
                )
                use_batch = False

        current_page_size = page_size
        while True:
            try:
                records = fetch_paginated_collection(
                    session,
                    base_url,
                    path,
                    "members",
                    params,
                    current_page_size,
                    max_attempts,
                    backoff_seconds,
                    progress_logger=lambda count, total, lid=list_id: logger.debug(
                        "Fetched %d/%s members for list %s",
                        count,
                        total if total else "unknown",
                        lid,
                    ),
                    progress_interval=10000,
                )
                logger.debug("Fetched %d members for list %s", len(records), list_id)
                for record in records:
                    record["list_id"] = list_id
                members.extend(records)
                break
            except RuntimeError as exc:
                error_message = str(exc)
                lowered = error_message.lower()
                timeout_condition = "read timed out" in lowered or "timeout" in lowered
                if (
                    timeout_condition
                    and current_page_size > DEFAULT_MEMBER_MIN_PAGE_SIZE
                ):
                    current_page_size = max(
                        DEFAULT_MEMBER_MIN_PAGE_SIZE,
                        current_page_size - DEFAULT_MEMBER_PAGE_SIZE_STEP,
                    )
                    logger.warning(
                        "Timeout fetching members for list %s; reducing page size to %s and retrying",
                        list_id,
                        current_page_size,
                    )
                    time.sleep(backoff_seconds)
                    continue
                failed_lists.append(list_id)
                logger.error(
                    "Failed to fetch members for list %s: %s", list_id, error_message
                )
                break

    if failed_lists:
        logger.warning(
            "Skipped %d Mailchimp lists due to repeated errors: %s",
            len(failed_lists),
            ", ".join(sorted(set(failed_lists))),
        )
    return normalize_records(members, default_fields)


def extract_unsubscribes(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    since = compute_since_timestamp(
        refresh_mode, lookback_days, start_date, table_config
    )
    filtered_campaigns, since_dt = get_filtered_sent_campaigns(
        session,
        base_url,
        cache,
        cache_key="campaigns_unsubscribes",
        since=since,
        page_size=page_size,
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        strategy="batch" if strategy == "batch" else "rest",
    )
    unsubscribes: List[Dict[str, Any]] = []
    total_campaigns = len(filtered_campaigns)
    logger.info("Processing %d campaigns for unsubscribe reports", total_campaigns)
    if total_campaigns == 0:
        logger.info("No campaigns to process for unsubscribes within lookback window")
        return normalize_records(unsubscribes, default_fields)
    if total_campaigns <= 100:
        campaign_summary_interval = 10
    else:
        campaign_summary_interval = max(1000, min(5000, total_campaigns // 20))
    row_summary_interval = 50000
    last_info_emit_idx = 0
    last_info_emit_rows = 0
    fetcher = (
        batch_fetch_paginated_collection
        if strategy == "batch"
        else fetch_paginated_collection
    )
    for idx, campaign in enumerate(filtered_campaigns, start=1):
        campaign_id = campaign.get("id")
        if not campaign_id:
            continue
        logger.debug(
            "[%d/%d] Fetching unsubscribes for campaign %s",
            idx,
            total_campaigns,
            campaign_id,
        )
        params: Dict[str, Any] = {}
        if since:
            params["since"] = since
        path = f"/reports/{campaign_id}/unsubscribed"
        try:
            records = fetcher(
                session,
                base_url,
                path,
                "unsubscribes",
                params,
                min(page_size, 200),
                max_attempts,
                backoff_seconds,
                progress_logger=lambda count, total, cid=campaign_id: logger.debug(
                    "Fetched %d/%s unsubscribes for campaign %s",
                    count,
                    total if total else "unknown",
                    cid,
                ),
                progress_interval=10000,
            )
        except RuntimeError as exc:
            if strategy == "batch":
                logger.warning(
                    "Batch unsubscribe fetch failed for campaign %s: %s; falling back to REST",
                    campaign_id,
                    exc,
                )
                records = fetch_paginated_collection(
                    session,
                    base_url,
                    path,
                    "unsubscribes",
                    params,
                    min(page_size, 200),
                    max_attempts,
                    backoff_seconds,
                    progress_logger=lambda count, total, cid=campaign_id: logger.debug(
                        "Fetched %d/%s unsubscribes for campaign %s",
                        count,
                        total if total else "unknown",
                        cid,
                    ),
                    progress_interval=10000,
                )
            else:
                raise
        accepted_records: List[Dict[str, Any]] = []
        for record in records:
            record["campaign_id"] = campaign_id
            recipients = campaign.get("recipients") or {}
            if recipients.get("list_id"):
                record.setdefault("list_id", recipients.get("list_id"))
            apply_merge_field_overrides(
                record,
                {
                    "first_name": ["FNAME", "FIRSTNAME", "FIRST_NAME"],
                    "last_name": ["LNAME", "LASTNAME", "LAST_NAME"],
                    "city": ["CITY"],
                    "state": ["STATE", "REGION"],
                    "country": ["COUNTRY"],
                    "subscriber_type": ["SUBSCRIBERTYPE", "SUBSCRIBER_TYPE"],
                },
            )
            if since_dt:
                unsub_dt = parse_mailchimp_timestamp(record.get("timestamp"))
                if unsub_dt and unsub_dt < since_dt:
                    continue
            accepted_records.append(record)
        unsubscribes.extend(accepted_records)
        total_collected = len(unsubscribes)
        should_emit_info = False
        if idx == total_campaigns:
            should_emit_info = True
        elif idx - last_info_emit_idx >= campaign_summary_interval:
            should_emit_info = True
        elif total_collected - last_info_emit_rows >= row_summary_interval:
            should_emit_info = True

        if should_emit_info:
            delta_rows = total_collected - last_info_emit_rows
            if delta_rows == 0 and idx != total_campaigns:
                logger.debug(
                    "[%d/%d] Campaign %s unsubscribes fetched: %d rows (total collected: %d)",
                    idx,
                    total_campaigns,
                    campaign_id,
                    len(accepted_records),
                    total_collected,
                )
            else:
                logger.info(
                    "[%d/%d] Campaign %s unsubscribes fetched: %d rows (total collected: %d)",
                    idx,
                    total_campaigns,
                    campaign_id,
                    delta_rows if idx != total_campaigns else len(accepted_records),
                    total_collected,
                )
                last_info_emit_idx = idx
                last_info_emit_rows = total_collected
        else:
            logger.debug(
                "[%d/%d] Campaign %s unsubscribes fetched: %d rows (total collected: %d)",
                idx,
                total_campaigns,
                campaign_id,
                len(accepted_records),
                total_collected,
            )

    logger.info(
        "Completed unsubscribe extraction: processed %d campaigns, collected %d rows",
        total_campaigns,
        len(unsubscribes),
    )
    return normalize_records(unsubscribes, default_fields)


def extract_campaign_email_activity(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    strategy = "rest"
    since = compute_since_timestamp(
        refresh_mode, lookback_days, start_date, table_config
    )
    filtered_campaigns, since_dt = get_filtered_sent_campaigns(
        session,
        base_url,
        cache,
        cache_key="campaigns_email_activity",
        since=since,
        page_size=page_size,
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        strategy="rest",
    )

    filtered_campaigns = filter_campaigns_for_table_activity(
        filtered_campaigns, "campaign_email_activity", logger
    )
    total_campaigns = len(filtered_campaigns)
    logger.info("Processing %d campaigns for email activity", total_campaigns)
    if total_campaigns == 0:
        return normalize_records([], default_fields)

    parallel_workers = cache.get("parallel_workers", 1)
    api_key = cache.get("api_key")
    if not api_key and session and session.auth:
        auth = session.auth
        if isinstance(auth, tuple) and len(auth) > 1:
            api_key = auth[1]
    request_timeout = cache.get(
        "timeout_seconds", getattr(session, "timeout", DEFAULT_TIMEOUT_SECONDS)
    )
    effective_workers = max(1, min(parallel_workers, MAILCHIMP_MAX_CONCURRENT_REQUESTS))

    campaign_milestone = max(1, total_campaigns // 10) if total_campaigns > 10 else 1
    row_summary_interval = 250_000
    rows: List[Dict[str, Any]] = []
    total_rows = 0
    completed = 0
    last_logged_campaigns = 0
    last_logged_rows = 0
    lock = threading.Lock()

    def should_emit(campaign_count: int, cumulative_rows: int) -> bool:
        if campaign_count == total_campaigns:
            return True
        if campaign_count - last_logged_campaigns >= campaign_milestone:
            return True
        if cumulative_rows - last_logged_rows >= row_summary_interval:
            return True
        return False

    def handle_progress(campaign_id: Optional[str], added_rows: int) -> None:
        nonlocal completed, total_rows, last_logged_campaigns, last_logged_rows
        completed += 1
        total_rows += added_rows
        if should_emit(completed, total_rows):
            logger.info(
                "[%d/%d] Campaign %s activity rows +%d (cumulative: %d)",
                completed,
                total_campaigns,
                campaign_id,
                added_rows,
                total_rows,
            )
            last_logged_campaigns = completed
            last_logged_rows = total_rows
        else:
            logger.debug(
                "[%d/%d] Campaign %s activity rows +%d (cumulative: %d)",
                completed,
                total_campaigns,
                campaign_id,
                added_rows,
                total_rows,
            )

    if effective_workers > 1 and api_key:
        logger.info(
            "Using %d parallel workers for campaign_email_activity", effective_workers
        )
        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            future_to_campaign = {
                executor.submit(
                    _process_campaign_email_activity,
                    campaign,
                    None,
                    base_url,
                    since,
                    since_dt,
                    page_size,
                    max_attempts,
                    backoff_seconds,
                    api_key,
                    request_timeout,
                ): campaign
                for campaign in filtered_campaigns
            }

            for future in as_completed(future_to_campaign):
                campaign = future_to_campaign[future]
                campaign_id = campaign.get("id") or "unknown"
                try:
                    campaign_rows = future.result()
                except Exception as exc:
                    logger.error(
                        "Campaign %s email activity failed: %s", campaign_id, exc
                    )
                    with lock:
                        handle_progress(campaign_id, 0)
                    continue

                added_rows = len(campaign_rows)
                with lock:
                    if campaign_rows:
                        rows.extend(campaign_rows)
                    handle_progress(campaign_id, added_rows)
    else:
        if effective_workers > 1 and not api_key:
            logger.debug(
                "Parallel workers requested but API key missing; defaulting to serial execution"
            )

        for campaign in filtered_campaigns:
            campaign_id = campaign.get("id") or "unknown"
            campaign_rows = _process_campaign_email_activity(
                campaign,
                session,
                base_url,
                since,
                since_dt,
                page_size,
                max_attempts,
                backoff_seconds,
                api_key,
                request_timeout,
            )
            added_rows = len(campaign_rows)
            if campaign_rows:
                rows.extend(campaign_rows)
            total_rows = len(rows)
            completed += 1
            if should_emit(completed, total_rows):
                logger.info(
                    "[%d/%d] Campaign %s activity rows +%d (cumulative: %d)",
                    completed,
                    total_campaigns,
                    campaign_id,
                    added_rows,
                    total_rows,
                )
                last_logged_campaigns = completed
                last_logged_rows = total_rows
            else:
                logger.debug(
                    "[%d/%d] Campaign %s activity rows +%d (cumulative: %d)",
                    completed,
                    total_campaigns,
                    campaign_id,
                    added_rows,
                    total_rows,
                )

    logger.info(
        "Completed email activity extraction: %d rows across %d campaigns",
        len(rows),
        total_campaigns,
    )
    return normalize_records(rows, default_fields)


def _process_campaign_sent_to(
    campaign: Dict[str, Any],
    session: Optional[requests.Session],
    base_url: str,
    since: Optional[str],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    api_key: Optional[str],
    request_timeout: int,
    default_fields: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Process a single campaign for sent-to recipients (thread-safe)."""
    campaign_id = campaign.get("id")
    if not campaign_id:
        return []

    # Skip campaigns with no recipients sent
    emails_sent = campaign.get("emails_sent") or 0
    if emails_sent == 0:
        return []

    worker_session = session
    if worker_session is None:
        if not api_key:
            raise ValueError("API key required for worker session")
        worker_session = get_worker_mailchimp_session(api_key, request_timeout)

    params = {"since": since} if since else None
    try:
        recipients = fetch_paginated_collection(
            worker_session,
            base_url,
            f"/reports/{campaign_id}/sent-to",
            "sent_to",
            params,
            page_size=min(page_size, 1000),
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
        )
    except Exception as exc:
        logger.warning("Failed to fetch sent-to for campaign %s: %s", campaign_id, exc)
        return []

    list_id = (campaign.get("recipients") or {}).get("list_id")
    rows = []
    for entry in recipients:
        row = dict(entry)
        row["campaign_id"] = campaign_id
        row.setdefault("list_id", list_id)
        if "subscriber_hash" not in row and row.get("email_address"):
            row["subscriber_hash"] = derive_email_id(campaign_id, row["email_address"])
        if default_fields:
            for key, value in default_fields.items():
                row.setdefault(key, value)
        rows.append(row)

    return rows


def extract_campaign_sent_to(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    strategy = "rest"
    since = compute_since_timestamp(
        refresh_mode, lookback_days, start_date, table_config
    )
    filtered_campaigns, _ = get_filtered_sent_campaigns(
        session,
        base_url,
        cache,
        cache_key="campaigns_sent_to",
        since=since,
        page_size=page_size,
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        strategy="rest",
    )
    total_campaigns = len(filtered_campaigns)
    logger.info("Processing %d campaigns for sent-to recipients", total_campaigns)
    if total_campaigns == 0:
        return []

    # Get parallel workers setting from cache
    parallel_workers = cache.get("parallel_workers", 1)
    api_key = cache.get("api_key")
    if not api_key and session and session.auth:
        auth = session.auth
        if isinstance(auth, tuple) and len(auth) > 1:
            api_key = auth[1]
    request_timeout = cache.get(
        "timeout_seconds", getattr(session, "timeout", DEFAULT_TIMEOUT_SECONDS)
    )
    effective_workers = max(1, min(parallel_workers, MAILCHIMP_MAX_CONCURRENT_REQUESTS))

    campaign_milestone = max(1, total_campaigns // 10) if total_campaigns > 10 else 1
    row_summary_interval = 250000
    rows: List[Dict[str, Any]] = []
    total_rows = 0
    completed = 0
    last_logged_campaigns = 0
    last_logged_rows = 0
    lock = threading.Lock()

    def should_emit(campaign_count: int, cumulative_rows: int) -> bool:
        if campaign_count == total_campaigns:
            return True
        if campaign_count - last_logged_campaigns >= campaign_milestone:
            return True
        if cumulative_rows - last_logged_rows >= row_summary_interval:
            return True
        return False

    def handle_progress(campaign_id: Optional[str], added_rows: int) -> None:
        nonlocal completed, total_rows, last_logged_campaigns, last_logged_rows
        completed += 1
        total_rows += added_rows
        if should_emit(completed, total_rows):
            logger.info(
                "[%d/%d] Campaign %s sent-to +%d (cumulative: %d)",
                completed,
                total_campaigns,
                campaign_id,
                added_rows,
                total_rows,
            )
            last_logged_campaigns = completed
            last_logged_rows = total_rows

    if effective_workers > 1 and api_key:
        logger.info(
            "Using %d parallel workers for campaign_sent_to extraction",
            effective_workers,
        )
        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            future_to_campaign = {
                executor.submit(
                    _process_campaign_sent_to,
                    campaign,
                    None,
                    base_url,
                    since,
                    page_size,
                    max_attempts,
                    backoff_seconds,
                    api_key,
                    request_timeout,
                    default_fields,
                ): campaign
                for campaign in filtered_campaigns
            }

            for future in as_completed(future_to_campaign):
                campaign = future_to_campaign[future]
                campaign_id = campaign.get("id") or "unknown"
                try:
                    campaign_rows = future.result()
                except Exception as exc:
                    logger.error("Campaign %s sent-to failed: %s", campaign_id, exc)
                    with lock:
                        handle_progress(campaign_id, 0)
                    continue

                added_rows = len(campaign_rows)
                with lock:
                    if campaign_rows:
                        rows.extend(campaign_rows)
                    handle_progress(campaign_id, added_rows)

        logger.info(
            "Completed sent-to extraction: %d rows across %d campaigns",
            len(rows),
            total_campaigns,
        )
        return normalize_records(rows, default_fields)

    else:
        if effective_workers > 1 and not api_key:
            logger.debug(
                "Parallel workers requested but API key missing; defaulting to serial execution"
            )

        # Serial fallback
        for idx, campaign in enumerate(filtered_campaigns, start=1):
            campaign_id = campaign.get("id") or "unknown"
            campaign_rows = _process_campaign_sent_to(
                campaign,
                session,
                base_url,
                since,
                page_size,
                max_attempts,
                backoff_seconds,
                api_key,
                request_timeout,
                default_fields,
            )
            added_rows = len(campaign_rows)
            if campaign_rows:
                rows.extend(campaign_rows)
            total_rows = len(rows)
            completed += 1
            if should_emit(completed, total_rows):
                logger.info(
                    "[%d/%d] Campaign %s sent-to +%d (cumulative: %d)",
                    completed,
                    total_campaigns,
                    campaign_id,
                    added_rows,
                    total_rows,
                )
                last_logged_campaigns = completed
                last_logged_rows = total_rows

        logger.info(
            "Completed sent-to extraction: %d rows across %d campaigns",
            len(rows),
            total_campaigns,
        )
        return normalize_records(rows, default_fields)


def _fetch_campaign_report(
    campaign_id: str,
    session: Optional[requests.Session],
    base_url: str,
    max_attempts: int,
    backoff_seconds: float,
    api_key: Optional[str],
    request_timeout: int,
) -> Optional[Dict[str, Any]]:
    """Fetch full /reports/{campaign_id} for a single campaign (thread-safe)."""
    if not campaign_id:
        return None

    worker_session = session
    if worker_session is None:
        if not api_key:
            raise ValueError("API key required for worker session")
        worker_session = get_worker_mailchimp_session(api_key, request_timeout)

    try:
        payload = mailchimp_request(
            worker_session,
            base_url,
            "GET",
            f"/reports/{campaign_id}",
            None,
            max_attempts,
            backoff_seconds,
        )
    except Exception as exc:
        logger.warning("Failed to fetch report for campaign %s: %s", campaign_id, exc)
        return None

    return payload


def _enrich_campaigns_with_reports(
    campaigns: List[Dict[str, Any]],
    session: requests.Session,
    base_url: str,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
) -> None:
    """Enrich campaign records in-place with full /reports/{id} data.

    Processes campaigns in batches with adaptive throttling to avoid
    overwhelming the Mailchimp API with sustained /reports requests.
    """
    total_campaigns = len(campaigns)
    if total_campaigns == 0:
        return

    api_key = cache.get("api_key")
    if not api_key and session and session.auth:
        auth = session.auth
        if isinstance(auth, tuple) and len(auth) > 1:
            api_key = auth[1]
    request_timeout = cache.get(
        "timeout_seconds", getattr(session, "timeout", DEFAULT_TIMEOUT_SECONDS)
    )
    # Fixed at 3 concurrent workers for report enrichment
    effective_workers = 3

    # Batch and throttle settings
    batch_size = 50  # campaigns per batch
    base_delay = 2.0  # seconds between batches
    max_delay = 60.0  # max backoff delay between batches
    consecutive_errors = 0  # track consecutive failures for backoff

    logger.info(
        "Enriching %d campaigns with full report data (%d workers, batches of %d)",
        total_campaigns,
        effective_workers,
        batch_size,
    )

    campaign_index = {}
    for i, c in enumerate(campaigns):
        cid = c.get("id")
        if cid:
            campaign_index[cid] = i

    campaign_ids = [c.get("id") for c in campaigns if c.get("id")]
    completed = 0
    enriched_count = 0
    failed_count = 0

    # Process in batches to avoid flooding the API
    for batch_start in range(0, len(campaign_ids), batch_size):
        batch = campaign_ids[batch_start : batch_start + batch_size]

        # Adaptive delay: back off when errors pile up, recover when clean
        if batch_start > 0:
            delay = min(base_delay * (2**consecutive_errors), max_delay)
            if consecutive_errors > 0:
                logger.info(
                    "Throttling: %.1fs delay before next batch (consecutive errors: %d)",
                    delay,
                    consecutive_errors,
                )
            time.sleep(delay)

        batch_errors = 0

        if effective_workers > 1 and api_key:
            lock = threading.Lock()
            with ThreadPoolExecutor(max_workers=effective_workers) as executor:
                future_to_id = {
                    executor.submit(
                        _fetch_campaign_report,
                        cid,
                        None,
                        base_url,
                        max_attempts,
                        backoff_seconds,
                        api_key,
                        request_timeout,
                    ): cid
                    for cid in batch
                }

                for future in as_completed(future_to_id):
                    campaign_id = future_to_id[future]
                    try:
                        report = future.result()
                    except Exception as exc:
                        logger.error(
                            "Campaign %s report fetch failed: %s", campaign_id, exc
                        )
                        report = None

                    with lock:
                        completed += 1
                        if report and campaign_id in campaign_index:
                            campaigns[campaign_index[campaign_id]][
                                "campaign_report"
                            ] = json.dumps(
                                report, default=serialize_datetime, sort_keys=True
                            )
                            enriched_count += 1
                        else:
                            if report is None:
                                batch_errors += 1
                                failed_count += 1
        else:
            for cid in batch:
                report = _fetch_campaign_report(
                    cid,
                    session,
                    base_url,
                    max_attempts,
                    backoff_seconds,
                    None,
                    request_timeout,
                )
                completed += 1
                if report and cid in campaign_index:
                    campaigns[campaign_index[cid]]["campaign_report"] = json.dumps(
                        report, default=serialize_datetime, sort_keys=True
                    )
                    enriched_count += 1
                else:
                    if report is None:
                        batch_errors += 1
                        failed_count += 1

        # Update consecutive error tracker for adaptive backoff
        if batch_errors > len(batch) * 0.3:
            consecutive_errors = min(consecutive_errors + 1, 5)
        elif batch_errors == 0:
            consecutive_errors = max(consecutive_errors - 1, 0)

        logger.info(
            "[%d/%d] Campaign reports fetched (%d enriched, %d failed)",
            completed,
            total_campaigns,
            enriched_count,
            failed_count,
        )

    logger.info(
        "Campaign report enrichment complete: %d/%d enriched, %d failed",
        enriched_count,
        total_campaigns,
        failed_count,
    )


def _process_campaign_domain_performance(
    campaign: Dict[str, Any],
    session: Optional[requests.Session],
    base_url: str,
    max_attempts: int,
    backoff_seconds: float,
    api_key: Optional[str],
    request_timeout: int,
) -> List[Dict[str, Any]]:
    """Process a single campaign for domain performance (thread-safe)."""
    campaign_id = campaign.get("id")
    if not campaign_id:
        return []

    worker_session = session
    if worker_session is None:
        if not api_key:
            raise ValueError("API key required for worker session")
        worker_session = get_worker_mailchimp_session(api_key, request_timeout)

    try:
        payload = mailchimp_request(
            worker_session,
            base_url,
            "GET",
            f"/reports/{campaign_id}/domain-performance",
            None,
            max_attempts,
            backoff_seconds,
        )
    except Exception as exc:
        logger.warning(
            "Failed to fetch domain performance for campaign %s: %s", campaign_id, exc
        )
        return []

    list_id = (campaign.get("recipients") or {}).get("list_id")
    rows = []
    for domain_entry in payload.get("domains", []) or []:
        row = dict(domain_entry)
        row["campaign_id"] = campaign_id
        row["list_id"] = list_id
        rows.append(row)

    return rows


def extract_campaign_domain_performance(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    since = compute_since_timestamp(
        refresh_mode, lookback_days, start_date, table_config
    )
    filtered_campaigns, _ = get_filtered_sent_campaigns(
        session,
        base_url,
        cache,
        cache_key="campaigns_domain_perf",
        since=since,
        page_size=page_size,
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        strategy="batch" if strategy == "batch" else "rest",
    )
    total_campaigns = len(filtered_campaigns)
    logger.info("Processing %d campaigns for domain performance", total_campaigns)
    if total_campaigns == 0:
        return []

    # Get parallel workers setting from cache
    parallel_workers = cache.get("parallel_workers", 1)
    api_key = cache.get("api_key")
    if not api_key and session and session.auth:
        auth = session.auth
        if isinstance(auth, tuple) and len(auth) > 1:
            api_key = auth[1]
    request_timeout = cache.get(
        "timeout_seconds", getattr(session, "timeout", DEFAULT_TIMEOUT_SECONDS)
    )
    effective_workers = max(1, min(parallel_workers, MAILCHIMP_MAX_CONCURRENT_REQUESTS))

    campaign_milestone = max(1, total_campaigns // 10) if total_campaigns > 10 else 1
    rows: List[Dict[str, Any]] = []
    total_rows = 0
    completed = 0
    last_logged_campaigns = 0
    lock = threading.Lock()

    def should_emit(campaign_count: int) -> bool:
        if campaign_count == total_campaigns:
            return True
        if campaign_count - last_logged_campaigns >= campaign_milestone:
            return True
        return False

    def handle_progress(campaign_id: Optional[str], added_rows: int) -> None:
        nonlocal completed, total_rows, last_logged_campaigns
        completed += 1
        total_rows += added_rows
        if should_emit(completed):
            logger.info(
                "[%d/%d] Campaign %s domain perf +%d (cumulative: %d)",
                completed,
                total_campaigns,
                campaign_id,
                added_rows,
                total_rows,
            )
            last_logged_campaigns = completed

    if effective_workers > 1 and api_key:
        logger.info(
            "Using %d parallel workers for campaign_domain_performance",
            effective_workers,
        )
        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            future_to_campaign = {
                executor.submit(
                    _process_campaign_domain_performance,
                    campaign,
                    None,
                    base_url,
                    max_attempts,
                    backoff_seconds,
                    api_key,
                    request_timeout,
                ): campaign
                for campaign in filtered_campaigns
            }

            for future in as_completed(future_to_campaign):
                campaign = future_to_campaign[future]
                campaign_id = campaign.get("id") or "unknown"
                try:
                    campaign_rows = future.result()
                except Exception as exc:
                    logger.error(
                        "Campaign %s domain performance failed: %s", campaign_id, exc
                    )
                    with lock:
                        handle_progress(campaign_id, 0)
                    continue

                added_rows = len(campaign_rows)
                with lock:
                    if campaign_rows:
                        rows.extend(campaign_rows)
                    handle_progress(campaign_id, added_rows)
    else:
        if effective_workers > 1 and not api_key:
            logger.debug(
                "Parallel workers requested but API key missing; defaulting to serial execution"
            )

        # Serial fallback
        for idx, campaign in enumerate(filtered_campaigns, start=1):
            campaign_id = campaign.get("id") or "unknown"
            campaign_rows = _process_campaign_domain_performance(
                campaign,
                session,
                base_url,
                max_attempts,
                backoff_seconds,
                api_key,
                request_timeout,
            )
            added_rows = len(campaign_rows)
            if campaign_rows:
                rows.extend(campaign_rows)
            total_rows = len(rows)
            completed += 1
            if should_emit(completed):
                logger.info(
                    "[%d/%d] Campaign %s domain perf +%d (cumulative: %d)",
                    completed,
                    total_campaigns,
                    campaign_id,
                    added_rows,
                    total_rows,
                )
                last_logged_campaigns = completed

    logger.info(
        "Completed domain performance extraction: %d rows across %d campaigns",
        len(rows),
        total_campaigns,
    )
    return normalize_records(rows, default_fields)


def _process_campaign_locations(
    campaign: Dict[str, Any],
    session: Optional[requests.Session],
    base_url: str,
    max_attempts: int,
    backoff_seconds: float,
    api_key: Optional[str],
    request_timeout: int,
) -> List[Dict[str, Any]]:
    """Process a single campaign for locations (thread-safe)."""
    campaign_id = campaign.get("id")
    if not campaign_id:
        return []

    worker_session = session
    if worker_session is None:
        if not api_key:
            raise ValueError("API key required for worker session")
        worker_session = get_worker_mailchimp_session(api_key, request_timeout)

    try:
        payload = mailchimp_request(
            worker_session,
            base_url,
            "GET",
            f"/reports/{campaign_id}/locations",
            None,
            max_attempts,
            backoff_seconds,
        )
    except Exception as exc:
        logger.warning(
            "Failed to fetch locations for campaign %s: %s", campaign_id, exc
        )
        return []

    list_id = (campaign.get("recipients") or {}).get("list_id")
    rows = []
    for entry in payload.get("locations", []) or []:
        row = dict(entry)
        row["campaign_id"] = campaign_id
        row["list_id"] = list_id
        rows.append(row)

    return rows


def extract_campaign_locations(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    since = compute_since_timestamp(
        refresh_mode, lookback_days, start_date, table_config
    )
    filtered_campaigns, _ = get_filtered_sent_campaigns(
        session,
        base_url,
        cache,
        cache_key="campaigns_locations",
        since=since,
        page_size=page_size,
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        strategy="batch" if strategy == "batch" else "rest",
    )
    total_campaigns = len(filtered_campaigns)
    logger.info("Processing %d campaigns for location metrics", total_campaigns)
    if total_campaigns == 0:
        return []

    # Get parallel workers setting from cache
    parallel_workers = cache.get("parallel_workers", 1)

    # IMPORTANT: campaign_locations endpoint is prone to 503 errors under load
    # Limit to 2 workers to avoid rate limit errors from Mailchimp
    if parallel_workers > 2:
        logger.info(
            "Reducing parallel workers from %d to 2 for campaign_locations (prevents API rate limiting)",
            parallel_workers,
        )
        parallel_workers = 2

    api_key = cache.get("api_key")
    if not api_key and session and session.auth:
        auth = session.auth
        if isinstance(auth, tuple) and len(auth) > 1:
            api_key = auth[1]
    request_timeout = cache.get(
        "timeout_seconds", getattr(session, "timeout", DEFAULT_TIMEOUT_SECONDS)
    )
    effective_workers = max(1, min(parallel_workers, MAILCHIMP_MAX_CONCURRENT_REQUESTS))

    campaign_milestone = max(1, total_campaigns // 10) if total_campaigns > 10 else 1
    rows: List[Dict[str, Any]] = []
    total_rows = 0
    completed = 0
    last_logged_campaigns = 0
    lock = threading.Lock()

    def should_emit(campaign_count: int) -> bool:
        if campaign_count == total_campaigns:
            return True
        if campaign_count - last_logged_campaigns >= campaign_milestone:
            return True
        return False

    def handle_progress(campaign_id: Optional[str], added_rows: int) -> None:
        nonlocal completed, total_rows, last_logged_campaigns
        completed += 1
        total_rows += added_rows
        if should_emit(completed):
            logger.info(
                "[%d/%d] Campaign %s locations +%d (cumulative: %d)",
                completed,
                total_campaigns,
                campaign_id,
                added_rows,
                total_rows,
            )
            last_logged_campaigns = completed

    if effective_workers > 1 and api_key:
        logger.info(
            "Using %d parallel workers for campaign_locations", effective_workers
        )
        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            future_to_campaign = {
                executor.submit(
                    _process_campaign_locations,
                    campaign,
                    None,
                    base_url,
                    max_attempts,
                    backoff_seconds,
                    api_key,
                    request_timeout,
                ): campaign
                for campaign in filtered_campaigns
            }

            for future in as_completed(future_to_campaign):
                campaign = future_to_campaign[future]
                campaign_id = campaign.get("id") or "unknown"
                try:
                    campaign_rows = future.result()
                except Exception as exc:
                    logger.error("Campaign %s locations failed: %s", campaign_id, exc)
                    with lock:
                        handle_progress(campaign_id, 0)
                    continue

                added_rows = len(campaign_rows)
                with lock:
                    if campaign_rows:
                        rows.extend(campaign_rows)
                    handle_progress(campaign_id, added_rows)
    else:
        if effective_workers > 1 and not api_key:
            logger.debug(
                "Parallel workers requested but API key missing; defaulting to serial execution"
            )

        # Serial fallback
        for idx, campaign in enumerate(filtered_campaigns, start=1):
            campaign_id = campaign.get("id") or "unknown"
            campaign_rows = _process_campaign_locations(
                campaign,
                session,
                base_url,
                max_attempts,
                backoff_seconds,
                api_key,
                request_timeout,
            )
            added_rows = len(campaign_rows)
            if campaign_rows:
                rows.extend(campaign_rows)
            total_rows = len(rows)
            completed += 1
            if should_emit(completed):
                logger.info(
                    "[%d/%d] Campaign %s locations +%d (cumulative: %d)",
                    completed,
                    total_campaigns,
                    campaign_id,
                    added_rows,
                    total_rows,
                )
                last_logged_campaigns = completed

    logger.info(
        "Completed location extraction: %d rows across %d campaigns",
        len(rows),
        total_campaigns,
    )
    return normalize_records(rows, default_fields)


def _process_campaign_open_details(
    campaign: Dict[str, Any],
    session: Optional[requests.Session],
    base_url: str,
    since: Optional[str],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    api_key: Optional[str],
    request_timeout: int,
) -> List[Dict[str, Any]]:
    """Process a single campaign for open details (thread-safe)."""
    campaign_id = campaign.get("id")
    if not campaign_id:
        return []

    worker_session = session
    if worker_session is None:
        if not api_key:
            raise ValueError("API key required for worker session")
        worker_session = get_worker_mailchimp_session(api_key, request_timeout)

    params = {"since": since} if since else None
    try:
        members = fetch_paginated_collection(
            worker_session,
            base_url,
            f"/reports/{campaign_id}/open-details",
            "members",
            params,
            page_size=min(page_size, 1000),
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
        )
    except Exception as exc:
        logger.warning(
            "Failed to fetch open details for campaign %s: %s", campaign_id, exc
        )
        return []

    list_id = (campaign.get("recipients") or {}).get("list_id")
    rows = []
    for member in members:
        row = dict(member)
        row["campaign_id"] = campaign_id
        row.setdefault("list_id", list_id)
        # Ensure subscriber_hash is always populated (part of PK); derive from email if missing/empty
        if not row.get("subscriber_hash") and row.get("email_address"):
            row["subscriber_hash"] = derive_email_id(campaign_id, row["email_address"])
        if not row.get("subscriber_hash"):
            continue  # Skip rows that cannot be uniquely identified
        rows.append(row)

    return rows


def extract_campaign_open_details(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    since = compute_since_timestamp(
        refresh_mode, lookback_days, start_date, table_config
    )
    filtered_campaigns, _ = get_filtered_sent_campaigns(
        session,
        base_url,
        cache,
        cache_key="campaigns_open_details",
        since=since,
        page_size=page_size,
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        strategy="batch" if strategy == "batch" else "rest",
    )
    filtered_campaigns = filter_campaigns_for_table_activity(
        filtered_campaigns, "campaign_open_details", logger
    )
    total_campaigns = len(filtered_campaigns)
    logger.info("Processing %d campaigns for open details", total_campaigns)
    if total_campaigns == 0:
        return []

    # Get parallel workers setting from cache
    parallel_workers = cache.get("parallel_workers", 1)
    api_key = cache.get("api_key")
    if not api_key and session and session.auth:
        auth = session.auth
        if isinstance(auth, tuple) and len(auth) > 1:
            api_key = auth[1]
    request_timeout = cache.get(
        "timeout_seconds", getattr(session, "timeout", DEFAULT_TIMEOUT_SECONDS)
    )
    effective_workers = max(1, min(parallel_workers, MAILCHIMP_MAX_CONCURRENT_REQUESTS))

    campaign_milestone = max(1, total_campaigns // 10) if total_campaigns > 10 else 1
    row_summary_interval = 50000
    rows: List[Dict[str, Any]] = []
    total_rows = 0
    completed = 0
    last_logged_campaigns = 0
    last_logged_rows = 0
    lock = threading.Lock()

    def should_emit(campaign_count: int, cumulative_rows: int) -> bool:
        if campaign_count == total_campaigns:
            return True
        if campaign_count - last_logged_campaigns >= campaign_milestone:
            return True
        if cumulative_rows - last_logged_rows >= row_summary_interval:
            return True
        return False

    def handle_progress(campaign_id: Optional[str], added_rows: int) -> None:
        nonlocal completed, total_rows, last_logged_campaigns, last_logged_rows
        completed += 1
        total_rows += added_rows
        if should_emit(completed, total_rows):
            logger.info(
                "[%d/%d] Campaign %s open details +%d (cumulative: %d)",
                completed,
                total_campaigns,
                campaign_id,
                added_rows,
                total_rows,
            )
            last_logged_campaigns = completed
            last_logged_rows = total_rows

    if effective_workers > 1 and api_key:
        logger.info(
            "Using %d parallel workers for campaign_open_details", effective_workers
        )
        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            future_to_campaign = {
                executor.submit(
                    _process_campaign_open_details,
                    campaign,
                    None,
                    base_url,
                    since,
                    page_size,
                    max_attempts,
                    backoff_seconds,
                    api_key,
                    request_timeout,
                ): campaign
                for campaign in filtered_campaigns
            }

            for future in as_completed(future_to_campaign):
                campaign = future_to_campaign[future]
                campaign_id = campaign.get("id") or "unknown"
                try:
                    campaign_rows = future.result()
                except Exception as exc:
                    logger.error(
                        "Campaign %s open details failed: %s", campaign_id, exc
                    )
                    with lock:
                        handle_progress(campaign_id, 0)
                    continue

                added_rows = len(campaign_rows)
                with lock:
                    if campaign_rows:
                        rows.extend(campaign_rows)
                    handle_progress(campaign_id, added_rows)
    else:
        if effective_workers > 1 and not api_key:
            logger.debug(
                "Parallel workers requested but API key missing; defaulting to serial execution"
            )

        # Serial fallback
        for idx, campaign in enumerate(filtered_campaigns, start=1):
            campaign_id = campaign.get("id") or "unknown"
            campaign_rows = _process_campaign_open_details(
                campaign,
                session,
                base_url,
                since,
                page_size,
                max_attempts,
                backoff_seconds,
                api_key,
                request_timeout,
            )
            added_rows = len(campaign_rows)
            if campaign_rows:
                rows.extend(campaign_rows)
            total_rows = len(rows)
            completed += 1
            if should_emit(completed, total_rows):
                logger.info(
                    "[%d/%d] Campaign %s open details +%d (cumulative: %d)",
                    completed,
                    total_campaigns,
                    campaign_id,
                    added_rows,
                    total_rows,
                )
                last_logged_campaigns = completed
                last_logged_rows = total_rows

    logger.info(
        "Completed open detail extraction: %d rows across %d campaigns",
        len(rows),
        total_campaigns,
    )
    return normalize_records(rows, default_fields)


def _process_campaign_click_details(
    campaign: Dict[str, Any],
    session: Optional[requests.Session],
    base_url: str,
    since: Optional[str],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    api_key: Optional[str],
    request_timeout: int,
) -> List[Dict[str, Any]]:
    """Process a single campaign for click details (thread-safe)."""
    campaign_id = campaign.get("id")
    if not campaign_id:
        return []

    worker_session = session
    if worker_session is None:
        if not api_key:
            raise ValueError("API key required for worker session")
        worker_session = get_worker_mailchimp_session(api_key, request_timeout)

    params = {"since": since} if since else None
    try:
        urls_clicked = fetch_paginated_collection(
            worker_session,
            base_url,
            f"/reports/{campaign_id}/click-details",
            "urls_clicked",
            params,
            page_size=min(page_size, 1000),
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
        )
    except Exception as exc:
        logger.warning(
            "Failed to fetch click details for campaign %s: %s", campaign_id, exc
        )
        return []

    list_id = (campaign.get("recipients") or {}).get("list_id")
    rows = []
    for entry in urls_clicked:
        row = dict(entry)
        row["campaign_id"] = campaign_id
        if "last_click" not in row:
            row["last_click"] = entry.get("last_clicked") or entry.get(
                "last_click_time"
            )
        row.setdefault("list_id", list_id)
        rows.append(row)

    return rows


def extract_campaign_click_details(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    since = compute_since_timestamp(
        refresh_mode, lookback_days, start_date, table_config
    )
    filtered_campaigns, _ = get_filtered_sent_campaigns(
        session,
        base_url,
        cache,
        cache_key="campaigns_click_details",
        since=since,
        page_size=page_size,
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        strategy="batch" if strategy == "batch" else "rest",
    )
    filtered_campaigns = filter_campaigns_for_table_activity(
        filtered_campaigns, "campaign_click_details", logger
    )
    total_campaigns = len(filtered_campaigns)
    logger.info("Processing %d campaigns for click details", total_campaigns)
    if total_campaigns == 0:
        return []

    # Get parallel workers setting from cache
    parallel_workers = cache.get("parallel_workers", 1)
    api_key = cache.get("api_key")
    if not api_key and session and session.auth:
        auth = session.auth
        if isinstance(auth, tuple) and len(auth) > 1:
            api_key = auth[1]
    request_timeout = cache.get(
        "timeout_seconds", getattr(session, "timeout", DEFAULT_TIMEOUT_SECONDS)
    )
    effective_workers = max(1, min(parallel_workers, MAILCHIMP_MAX_CONCURRENT_REQUESTS))

    campaign_milestone = max(1, total_campaigns // 10) if total_campaigns > 10 else 1
    row_summary_interval = 50000
    rows: List[Dict[str, Any]] = []
    total_rows = 0
    completed = 0
    last_logged_campaigns = 0
    last_logged_rows = 0
    lock = threading.Lock()

    def should_emit(campaign_count: int, cumulative_rows: int) -> bool:
        if campaign_count == total_campaigns:
            return True
        if campaign_count - last_logged_campaigns >= campaign_milestone:
            return True
        if cumulative_rows - last_logged_rows >= row_summary_interval:
            return True
        return False

    def handle_progress(campaign_id: Optional[str], added_rows: int) -> None:
        nonlocal completed, total_rows, last_logged_campaigns, last_logged_rows
        completed += 1
        total_rows += added_rows
        if should_emit(completed, total_rows):
            logger.info(
                "[%d/%d] Campaign %s click details +%d (cumulative: %d)",
                completed,
                total_campaigns,
                campaign_id,
                added_rows,
                total_rows,
            )
            last_logged_campaigns = completed
            last_logged_rows = total_rows

    if effective_workers > 1 and api_key:
        logger.info(
            "Using %d parallel workers for campaign_click_details", effective_workers
        )
        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            future_to_campaign = {
                executor.submit(
                    _process_campaign_click_details,
                    campaign,
                    None,
                    base_url,
                    since,
                    page_size,
                    max_attempts,
                    backoff_seconds,
                    api_key,
                    request_timeout,
                ): campaign
                for campaign in filtered_campaigns
            }

            for future in as_completed(future_to_campaign):
                campaign = future_to_campaign[future]
                campaign_id = campaign.get("id") or "unknown"
                try:
                    campaign_rows = future.result()
                except Exception as exc:
                    logger.error(
                        "Campaign %s click details failed: %s", campaign_id, exc
                    )
                    with lock:
                        handle_progress(campaign_id, 0)
                    continue

                added_rows = len(campaign_rows)
                with lock:
                    if campaign_rows:
                        rows.extend(campaign_rows)
                    handle_progress(campaign_id, added_rows)
    else:
        if effective_workers > 1 and not api_key:
            logger.debug(
                "Parallel workers requested but API key missing; defaulting to serial execution"
            )

        # Serial fallback
        for idx, campaign in enumerate(filtered_campaigns, start=1):
            campaign_id = campaign.get("id") or "unknown"
            campaign_rows = _process_campaign_click_details(
                campaign,
                session,
                base_url,
                since,
                page_size,
                max_attempts,
                backoff_seconds,
                api_key,
                request_timeout,
            )
            added_rows = len(campaign_rows)
            if campaign_rows:
                rows.extend(campaign_rows)
            total_rows = len(rows)
            completed += 1
            if should_emit(completed, total_rows):
                logger.info(
                    "[%d/%d] Campaign %s click details +%d (cumulative: %d)",
                    completed,
                    total_campaigns,
                    campaign_id,
                    added_rows,
                    total_rows,
                )
                last_logged_campaigns = completed
                last_logged_rows = total_rows

    logger.info(
        "Completed click detail extraction: %d rows across %d campaigns",
        len(rows),
        total_campaigns,
    )
    return normalize_records(rows, default_fields)


def _process_campaign_click_members(
    campaign: Dict[str, Any],
    session: Optional[requests.Session],
    base_url: str,
    since: Optional[str],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    api_key: Optional[str],
    request_timeout: int,
) -> List[Dict[str, Any]]:
    """Process a single campaign for click members (thread-safe)."""
    campaign_id = campaign.get("id")
    if not campaign_id:
        return []

    worker_session = session
    if worker_session is None:
        if not api_key:
            raise ValueError("API key required for worker session")
        worker_session = get_worker_mailchimp_session(api_key, request_timeout)

    params = {"since": since} if since else None
    try:
        urls_clicked = fetch_paginated_collection(
            worker_session,
            base_url,
            f"/reports/{campaign_id}/click-details",
            "urls_clicked",
            params,
            page_size=min(page_size, 1000),
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
        )
    except Exception as exc:
        logger.warning(
            "Failed to fetch click details for campaign %s: %s", campaign_id, exc
        )
        return []

    list_id = (campaign.get("recipients") or {}).get("list_id")
    rows = []
    for link_entry in urls_clicked:
        link_id = link_entry.get("id") or link_entry.get("link_id")
        if not link_id:
            continue
        try:
            members = fetch_paginated_collection(
                worker_session,
                base_url,
                f"/reports/{campaign_id}/click-details/{link_id}/members",
                "members",
                params,
                page_size=min(page_size, 1000),
                max_attempts=max_attempts,
                backoff_seconds=backoff_seconds,
            )
            for member in members:
                row = dict(member)
                row["campaign_id"] = campaign_id
                row["link_id"] = link_id
                row.setdefault("list_id", list_id)
                if "subscriber_hash" not in row and row.get("email_address"):
                    row["subscriber_hash"] = derive_email_id(
                        campaign_id, row["email_address"]
                    )
                rows.append(row)
        except Exception as exc:
            logger.warning(
                "Failed to fetch members for campaign %s link %s: %s",
                campaign_id,
                link_id,
                exc,
            )

    return rows


def extract_campaign_click_members(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    since = compute_since_timestamp(
        refresh_mode, lookback_days, start_date, table_config
    )
    filtered_campaigns, _ = get_filtered_sent_campaigns(
        session,
        base_url,
        cache,
        cache_key="campaigns_click_members",
        since=since,
        page_size=page_size,
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        strategy="batch" if strategy == "batch" else "rest",
    )
    filtered_campaigns = filter_campaigns_for_table_activity(
        filtered_campaigns, "campaign_click_members", logger
    )
    total_campaigns = len(filtered_campaigns)
    logger.info("Processing %d campaigns for click members", total_campaigns)
    if total_campaigns == 0:
        return []

    # Get parallel workers setting from cache
    parallel_workers = cache.get("parallel_workers", 1)

    # IMPORTANT: campaign_click_members makes MANY API calls per campaign (one per link)
    # Limit to 2 workers to avoid 503 rate limit errors from Mailchimp
    if parallel_workers > 2:
        logger.info(
            "Reducing parallel workers from %d to 2 for campaign_click_members (prevents API rate limiting)",
            parallel_workers,
        )
        parallel_workers = 2

    api_key = cache.get("api_key")
    if not api_key and session and session.auth:
        auth = session.auth
        if isinstance(auth, tuple) and len(auth) > 1:
            api_key = auth[1]
    request_timeout = cache.get(
        "timeout_seconds", getattr(session, "timeout", DEFAULT_TIMEOUT_SECONDS)
    )
    effective_workers = max(1, min(parallel_workers, MAILCHIMP_MAX_CONCURRENT_REQUESTS))

    campaign_milestone = max(1, total_campaigns // 10) if total_campaigns > 10 else 1
    row_summary_interval = 50000
    rows: List[Dict[str, Any]] = []
    total_rows = 0
    completed = 0
    last_logged_campaigns = 0
    last_logged_rows = 0
    lock = threading.Lock()

    def should_emit(campaign_count: int, cumulative_rows: int) -> bool:
        if campaign_count == total_campaigns:
            return True
        if campaign_count - last_logged_campaigns >= campaign_milestone:
            return True
        if cumulative_rows - last_logged_rows >= row_summary_interval:
            return True
        return False

    def handle_progress(campaign_id: Optional[str], added_rows: int) -> None:
        nonlocal completed, total_rows, last_logged_campaigns, last_logged_rows
        completed += 1
        total_rows += added_rows
        if should_emit(completed, total_rows):
            logger.info(
                "[%d/%d] Campaign %s click members +%d (cumulative: %d)",
                completed,
                total_campaigns,
                campaign_id,
                added_rows,
                total_rows,
            )
            last_logged_campaigns = completed
            last_logged_rows = total_rows

    if effective_workers > 1 and api_key:
        logger.info(
            "Using %d parallel workers for campaign_click_members", effective_workers
        )
        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            future_to_campaign = {
                executor.submit(
                    _process_campaign_click_members,
                    campaign,
                    None,
                    base_url,
                    since,
                    page_size,
                    max_attempts,
                    backoff_seconds,
                    api_key,
                    request_timeout,
                ): campaign
                for campaign in filtered_campaigns
            }

            for future in as_completed(future_to_campaign):
                campaign = future_to_campaign[future]
                campaign_id = campaign.get("id") or "unknown"
                try:
                    campaign_rows = future.result()
                except Exception as exc:
                    logger.error(
                        "Campaign %s click members failed: %s", campaign_id, exc
                    )
                    with lock:
                        handle_progress(campaign_id, 0)
                    continue

                added_rows = len(campaign_rows)
                with lock:
                    if campaign_rows:
                        rows.extend(campaign_rows)
                    handle_progress(campaign_id, added_rows)
    else:
        if effective_workers > 1 and not api_key:
            logger.debug(
                "Parallel workers requested but API key missing; defaulting to serial execution"
            )

        # Serial fallback
        for idx, campaign in enumerate(filtered_campaigns, start=1):
            campaign_id = campaign.get("id") or "unknown"
            campaign_rows = _process_campaign_click_members(
                campaign,
                session,
                base_url,
                since,
                page_size,
                max_attempts,
                backoff_seconds,
                api_key,
                request_timeout,
            )
            added_rows = len(campaign_rows)
            if campaign_rows:
                rows.extend(campaign_rows)
            total_rows = len(rows)
            completed += 1
            if should_emit(completed, total_rows):
                logger.info(
                    "[%d/%d] Campaign %s click members +%d (cumulative: %d)",
                    completed,
                    total_campaigns,
                    campaign_id,
                    added_rows,
                    total_rows,
                )
                last_logged_campaigns = completed
                last_logged_rows = total_rows

    logger.info(
        "Completed click member extraction: %d rows across %d campaigns",
        len(rows),
        total_campaigns,
    )
    return normalize_records(rows, default_fields)


def extract_list_merge_fields(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    lists = cache.get("lists_raw")
    if not lists:
        lists = fetch_paginated_collection(
            session,
            base_url,
            "/lists",
            "lists",
            {},
            page_size,
            max_attempts,
            backoff_seconds,
        )
        cache["lists_raw"] = lists
    rows: List[Dict[str, Any]] = []
    for lst in lists:
        list_id = lst.get("id")
        if not list_id:
            continue
        merge_fields = fetch_paginated_collection(
            session,
            base_url,
            f"/lists/{list_id}/merge-fields",
            "merge_fields",
            None,
            page_size=min(page_size, 250),
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
        )
        for field in merge_fields:
            row = dict(field)
            row["list_id"] = list_id
            rows.append(row)
    logger.info("Collected %d merge field rows across %d lists", len(rows), len(lists))
    return normalize_records(rows, default_fields)


def extract_list_growth_history(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    lists = cache.get("lists_raw")
    if not lists:
        lists = fetch_paginated_collection(
            session,
            base_url,
            "/lists",
            "lists",
            {},
            page_size,
            max_attempts,
            backoff_seconds,
        )
        cache["lists_raw"] = lists
    rows: List[Dict[str, Any]] = []
    int_metrics = [
        "existing",
        "imports",
        "optins",
        "unsubscribes",
        "cleaned",
        "member_count",
    ]
    string_metrics = [
        "subscribed",
        "unsubscribed",
        "reconfirm",
        "pending",
        "deleted",
        "transactional",
    ]

    for lst in lists:
        list_id = lst.get("id")
        if not list_id:
            continue
        payload = mailchimp_request(
            session,
            base_url,
            "GET",
            f"/lists/{list_id}/growth-history",
            None,
            max_attempts,
            backoff_seconds,
        )
        for entry in payload.get("history", []) or []:
            row = dict(entry)
            row["list_id"] = list_id
            for metric_key in int_metrics:
                if metric_key in row:
                    coerced = safe_int(row.get(metric_key))
                    row[metric_key] = coerced if coerced is not None else 0

            for metric_key in string_metrics:
                if metric_key in row and row[metric_key] not in (None, ""):
                    row[metric_key] = str(row[metric_key])
            rows.append(row)
    logger.info(
        "Collected %d growth history rows across %d lists", len(rows), len(lists)
    )
    return normalize_records(rows, default_fields)


def extract_list_activity(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    lists = cache.get("lists_raw")
    if not lists:
        lists = fetch_paginated_collection(
            session,
            base_url,
            "/lists",
            "lists",
            {},
            page_size,
            max_attempts,
            backoff_seconds,
        )
        cache["lists_raw"] = lists
    rows: List[Dict[str, Any]] = []
    for lst in lists:
        list_id = lst.get("id")
        if not list_id:
            continue
        payload = mailchimp_request(
            session,
            base_url,
            "GET",
            f"/lists/{list_id}/activity",
            None,
            max_attempts,
            backoff_seconds,
        )
        for entry in payload.get("activity", []) or []:
            row = dict(entry)
            row["list_id"] = list_id
            day_value = row.get("day") or row.get("day_of_week")
            parsed_day = parse_mailchimp_date(day_value if day_value else None)
            if parsed_day:
                row["day"] = parsed_day
            else:
                row["day"] = None
            for metric_key in [
                "emails_sent",
                "unique_opens",
                "recipient_clicks",
                "hard_bounce",
                "soft_bounce",
                "subs",
                "unsubs",
                "other_adds",
                "other_removes",
            ]:
                if metric_key in row:
                    coerced = safe_int(row.get(metric_key))
                    row[metric_key] = coerced if coerced is not None else 0
            rows.append(row)
    logger.info(
        "Collected %d list activity rows across %d lists", len(rows), len(lists)
    )
    return normalize_records(rows, default_fields)


def extract_list_segments(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    window_start_dt, window_end_dt = resolve_run_window(
        refresh_mode,
        lookback_days,
        cache.get("explicit_start_date") or start_date,
        cache.get("explicit_end_date"),
    )

    lists = cache.get("lists_raw")
    if not lists:
        lists = fetch_paginated_collection(
            session,
            base_url,
            "/lists",
            "lists",
            {},
            page_size,
            max_attempts,
            backoff_seconds,
        )
        cache["lists_raw"] = lists
    rows: List[Dict[str, Any]] = []
    segment_cache = cache.setdefault("list_segments", {})
    for lst in lists:
        list_id = lst.get("id")
        if not list_id:
            continue
        segment_params: Optional[Dict[str, Any]] = {}
        if window_start_dt:
            segment_params["since_last_changed"] = window_start_dt.isoformat()
        segments = fetch_paginated_collection(
            session,
            base_url,
            f"/lists/{list_id}/segments",
            "segments",
            segment_params,
            page_size=min(page_size, 100),
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
        )
        segment_cache[list_id] = segments
        for segment in segments:
            row = dict(segment)
            row["list_id"] = list_id
            row["segment_id"] = row.get("id") or row.get("segment_id")
            if "member_count" in row:
                coerced = safe_int(row.get("member_count"))
                row["member_count"] = coerced if coerced is not None else 0
            created_at_raw = row.get("created_at") or row.get("create_time")
            updated_at_raw = row.get("updated_at") or row.get("update_time")
            parsed_created = (
                parse_mailchimp_timestamp(created_at_raw) if created_at_raw else None
            )
            parsed_updated = (
                parse_mailchimp_timestamp(updated_at_raw) if updated_at_raw else None
            )
            if parsed_created:
                row["created_at"] = parsed_created.astimezone(timezone.utc).replace(
                    second=0, microsecond=0, tzinfo=None
                )
            else:
                row["created_at"] = None
            if parsed_updated:
                row["updated_at"] = parsed_updated.astimezone(timezone.utc).replace(
                    second=0, microsecond=0, tzinfo=None
                )
            else:
                row["updated_at"] = None
            if window_start_dt or window_end_dt:
                candidate_dt = parsed_updated or parsed_created
                if candidate_dt:
                    if window_start_dt and candidate_dt < window_start_dt:
                        continue
                    if window_end_dt and candidate_dt > window_end_dt:
                        continue
            rows.append(row)
    logger.info("Collected %d segment rows across %d lists", len(rows), len(lists))
    normalized = normalize_records(rows, default_fields)

    for row in normalized:
        for field in ("created_at", "updated_at"):
            value = row.get(field)
            if not value:
                row[field] = None
                continue
            parsed_dt = parse_mailchimp_timestamp(value)
            if parsed_dt:
                row[field] = parsed_dt.astimezone(timezone.utc).replace(
                    second=0, microsecond=0, tzinfo=None
                )
            else:
                row[field] = None

    return normalized


def extract_list_segment_members(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    # IMPORTANT: list_segment_members is a snapshot table (current segment membership state)
    # Date ranges don't apply - we always fetch the current state
    # Ignore start_date/end_date parameters for this table
    logger.info(
        "Note: list_segment_members is a snapshot table - date ranges are ignored"
    )
    window_start_dt, window_end_dt = None, None

    batch_config = (
        table_config.get("batch", {})
        if isinstance(table_config.get("batch"), dict)
        else {}
    )
    batch_timeout = batch_config.get("timeout_seconds", DEFAULT_BATCH_TIMEOUT_SECONDS)
    batch_poll = batch_config.get("poll_interval", DEFAULT_BATCH_POLL_INTERVAL_SECONDS)
    lists = cache.get("lists_raw")
    if not lists:
        list_fetcher = (
            batch_fetch_paginated_collection
            if strategy == "batch"
            else fetch_paginated_collection
        )
        try:
            lists = list_fetcher(
                session,
                base_url,
                "/lists",
                "lists",
                {},
                page_size,
                max_attempts,
                backoff_seconds,
                poll_interval=batch_poll,
                timeout_seconds=batch_timeout,
            )
        except RuntimeError as exc:
            if strategy == "batch":
                logger.warning(
                    "Batch list fetch failed (%s); falling back to REST", exc
                )
                lists = fetch_paginated_collection(
                    session,
                    base_url,
                    "/lists",
                    "lists",
                    {},
                    page_size,
                    max_attempts,
                    backoff_seconds,
                )
                strategy = "rest"
            else:
                raise
        cache["lists_raw"] = lists
    rows: List[Dict[str, Any]] = []
    segment_cache = cache.setdefault("list_segments", {})
    use_batch = strategy == "batch"

    logger.info(f"Processing {len(lists)} lists for segment members...")

    for list_idx, lst in enumerate(lists, 1):
        list_id = lst.get("id")
        if not list_id:
            continue

        list_name = lst.get("name", list_id)
        logger.info(
            f"[{list_idx}/{len(lists)}] Processing list: {list_name} ({list_id})"
        )

        segments = segment_cache.get(list_id)
        if segments is None:
            segment_path = f"/lists/{list_id}/segments"
            segment_page_size = min(page_size, 100)
            # No date filtering for segments - fetch all segments
            segment_params: Optional[Dict[str, Any]] = {}
            if use_batch:
                try:
                    segments = batch_fetch_paginated_collection(
                        session,
                        base_url,
                        segment_path,
                        "segments",
                        segment_params,
                        page_size=segment_page_size,
                        max_attempts=max_attempts,
                        backoff_seconds=backoff_seconds,
                        poll_interval=batch_poll,
                        timeout_seconds=batch_timeout,
                    )
                except RuntimeError as exc:
                    logger.warning(
                        "Batch segment fetch failed for list %s: %s; falling back to REST",
                        list_id,
                        exc,
                    )
                    segments = fetch_paginated_collection(
                        session,
                        base_url,
                        segment_path,
                        "segments",
                        segment_params,
                        page_size=segment_page_size,
                        max_attempts=max_attempts,
                        backoff_seconds=backoff_seconds,
                    )
                    use_batch = False
            else:
                segments = fetch_paginated_collection(
                    session,
                    base_url,
                    segment_path,
                    "segments",
                    segment_params or None,
                    page_size=segment_page_size,
                    max_attempts=max_attempts,
                    backoff_seconds=backoff_seconds,
                )
            segment_cache[list_id] = segments

        logger.info(f"  Found {len(segments)} segments in list {list_name}")

        # Check for max_segment_size limit in table config
        max_segment_size = table_config.get("max_segment_size", None)

        for seg_idx, segment in enumerate(segments, 1):
            segment_id = segment.get("id")
            if not segment_id:
                continue

            segment_name = segment.get("name", segment_id)
            member_count = segment.get("member_count", 0)

            # Skip segments that are too large to prevent timeouts
            if max_segment_size and member_count > max_segment_size:
                logger.warning(
                    f"  [{seg_idx}/{len(segments)}] SKIPPING large segment: {segment_name} ({member_count:,} members > {max_segment_size:,} limit)"
                )
                continue

            logger.info(
                f"  [{seg_idx}/{len(segments)}] Fetching segment: {segment_name} ({member_count:,} members)"
            )

            member_path = f"/lists/{list_id}/segments/{segment_id}/members"
            member_page_size = min(page_size, 500)
            # No date filtering for members - fetch all current segment members
            member_params: Optional[Dict[str, Any]] = {}
            if use_batch:
                try:
                    members = batch_fetch_paginated_collection(
                        session,
                        base_url,
                        member_path,
                        "members",
                        member_params,
                        page_size=member_page_size,
                        max_attempts=max_attempts,
                        backoff_seconds=backoff_seconds,
                        poll_interval=batch_poll,
                        timeout_seconds=batch_timeout,
                    )
                except RuntimeError as exc:
                    logger.warning(
                        "Batch member fetch failed for list %s segment %s: %s; falling back to REST",
                        list_id,
                        segment_id,
                        exc,
                    )
                    members = fetch_paginated_collection(
                        session,
                        base_url,
                        member_path,
                        "members",
                        member_params,
                        page_size=member_page_size,
                        max_attempts=max_attempts,
                        backoff_seconds=backoff_seconds,
                    )
                    use_batch = False
            else:
                members = fetch_paginated_collection(
                    session,
                    base_url,
                    member_path,
                    "members",
                    member_params,
                    page_size=member_page_size,
                    max_attempts=max_attempts,
                    backoff_seconds=backoff_seconds,
                )

            logger.info(
                f"    Retrieved {len(members)} members from segment {segment_name}"
            )

            for member in members:
                row = dict(member)
                row["list_id"] = list_id
                row["segment_id"] = segment_id
                if "subscriber_hash" not in row and row.get("email_address"):
                    row["subscriber_hash"] = hashlib.md5(
                        row["email_address"].lower().encode("utf-8")
                    ).hexdigest()
                # No date filtering - this is a snapshot of current state
                updated_raw = (
                    row.get("last_changed")
                    or row.get("timestamp_opt")
                    or row.get("timestamp_added")
                )
                if updated_raw:
                    parsed_updated = parse_mailchimp_timestamp(updated_raw)
                    if parsed_updated:
                        row["updated_at"] = parsed_updated.astimezone(
                            timezone.utc
                        ).replace(second=0, microsecond=0, tzinfo=None)
                    else:
                        row["updated_at"] = None
                else:
                    row["updated_at"] = None
                rows.append(row)

        logger.info(
            f"  List {list_name} complete: {len([r for r in rows if r['list_id'] == list_id])} total members"
        )

    logger.info(
        f"Collected {len(rows):,} segment member rows across {len(lists)} lists"
    )
    normalized = normalize_records(rows, default_fields)

    for row in normalized:
        value = row.get("updated_at")
        if not value:
            row["updated_at"] = None
            continue
        parsed_dt = parse_mailchimp_timestamp(value)
        if parsed_dt:
            row["updated_at"] = parsed_dt.astimezone(timezone.utc).replace(
                second=0, microsecond=0, tzinfo=None
            )
        else:
            row["updated_at"] = None

    return normalized


def extract_bounces(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    table_config: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    cache: Dict[str, Any],
    default_fields: Optional[Dict[str, Any]] = None,
    strategy: str = "rest",
) -> List[Dict[str, Any]]:
    since = compute_since_timestamp(
        refresh_mode, lookback_days, start_date, table_config
    )
    campaigns = get_campaigns(
        session,
        base_url,
        cache,
        cache_key="campaigns_bounces",
        since=since,
        page_size=page_size,
        max_attempts=max_attempts,
        backoff_seconds=backoff_seconds,
        fields="campaigns.id,campaigns.recipients,campaigns.send_time,campaigns.status,campaigns.emails_sent",
        extra_params={"status": "sent"},
        strategy="batch" if strategy == "batch" else "rest",
    )
    filtered_campaigns = []
    since_dt = parse_mailchimp_timestamp(since)
    logger.info("Filtering campaigns for bounce lookback: initial=%d", len(campaigns))
    for campaign in campaigns:
        send_time = campaign.get("send_time")
        campaign_dt = parse_mailchimp_timestamp(send_time)
        if since_dt and campaign_dt is None:
            continue
        if since_dt and campaign_dt:
            logger.debug(
                "Campaign %s send_time=%s (since=%s)",
                campaign.get("id"),
                send_time,
                since,
            )
            if campaign_dt < since_dt:
                continue
        filtered_campaigns.append(campaign)
    campaign_filters = cache.get("campaign_filters")
    # Skip date filtering from config if explicit 'since' date was provided
    skip_date_filtering = since is not None
    explicit_start = cache.get("explicit_start_date")
    explicit_end = cache.get("explicit_end_date")
    filtered_campaigns = filter_campaigns_by_date_and_activity(
        filtered_campaigns,
        campaign_filters,
        logger,
        skip_date_filtering,
        explicit_start,
        explicit_end,
    )
    logger.info("Campaigns within bounce lookback window: %d", len(filtered_campaigns))

    bounces: List[Dict[str, Any]] = []
    total_campaigns = len(filtered_campaigns)
    logger.info("Processing %d campaigns for bounce reports", total_campaigns)
    if total_campaigns == 0:
        logger.info("No campaigns to process for bounces within lookback window")
        return normalize_records(bounces, default_fields)
    if total_campaigns <= 100:
        campaign_summary_interval = 10
    else:
        campaign_summary_interval = max(1000, min(5000, total_campaigns // 20))
    row_summary_interval = 50000
    last_info_emit_idx = 0
    last_info_emit_rows = 0
    for idx, campaign in enumerate(filtered_campaigns, start=1):
        campaign_id = campaign.get("id")
        if not campaign_id:
            continue
        logger.debug(
            "[%d/%d] Fetching bounces for campaign %s",
            idx,
            total_campaigns,
            campaign_id,
        )
        params: Dict[str, Any] = {"count": min(page_size, 200), "offset": 0}
        if since:
            params["since"] = since
        offset = 0
        campaign_start_count = len(bounces)
        accepted_start = len(bounces)
        while True:
            params["offset"] = offset
            logger.debug(
                "[%d/%d] Campaign %s requesting email-activity offset=%s count=%s",
                idx,
                total_campaigns,
                campaign_id,
                offset,
                params["count"],
            )
            request_start = time.time()
            payload = mailchimp_request(
                session,
                base_url,
                "GET",
                f"/reports/{campaign_id}/email-activity",
                params,
                max_attempts,
                backoff_seconds,
            )
            emails = payload.get("emails", [])
            elapsed = time.time() - request_start
            logger.debug(
                "[%d/%d] Campaign %s offset=%s returned %d emails in %.2fs",
                idx,
                total_campaigns,
                campaign_id,
                offset,
                len(emails),
                elapsed,
            )
            if not emails:
                break
            page_has_recent = False
            for email_record in emails:
                activities = email_record.get("activity", []) or []
                for activity in activities:
                    # Mailchimp API may return action='bounce' or type='hard'/'soft' for bounces
                    act = (activity.get("action") or "").lower()
                    typ = (activity.get("type") or "").lower()
                    is_bounce = act == "bounce" or (not act and typ in ("hard", "soft"))
                    if is_bounce:
                        bounce_entry = {
                            "campaign_id": campaign_id,
                            "email_id": email_record.get("email_id"),
                            "email_address": email_record.get("email_address"),
                            "bounce_type": activity.get("type"),
                            "bounce_timestamp": activity.get("timestamp"),
                            "ip": activity.get("ip"),
                            "url": activity.get("url"),
                        }
                        bounce_dt = parse_mailchimp_timestamp(
                            bounce_entry["bounce_timestamp"]
                        )
                        if since_dt:
                            if bounce_dt and bounce_dt < since_dt:
                                logger.debug(
                                    "Skipping bounce %s older than %s",
                                    bounce_entry["bounce_timestamp"],
                                    since,
                                )
                                continue
                            if bounce_dt:
                                page_has_recent = True
                            else:
                                page_has_recent = True
                        else:
                            page_has_recent = True
                        recipients = campaign.get("recipients") or {}
                        if recipients.get("list_id"):
                            bounce_entry["list_id"] = recipients.get("list_id")
                        bounces.append(bounce_entry)
                        page_has_recent = True
            if since_dt and not page_has_recent:
                logger.debug(
                    "[%d/%d] Campaign %s page offset=%s contains no bounces after %s; stopping pagination",
                    idx,
                    total_campaigns,
                    campaign_id,
                    offset,
                    since,
                )
                break
            offset += params["count"]
            total_items = payload.get("total_items", offset)
            if offset >= total_items:
                break
        campaign_new_rows = len(bounces) - accepted_start
        total_collected = len(bounces)
        should_emit_info = False
        if idx == total_campaigns:
            should_emit_info = True
        elif idx - last_info_emit_idx >= campaign_summary_interval:
            should_emit_info = True
        elif total_collected - last_info_emit_rows >= row_summary_interval:
            should_emit_info = True

        if should_emit_info:
            logger.info(
                "[%d/%d] Campaign %s bounces fetched: %d rows (cumulative total: %d)",
                idx,
                total_campaigns,
                campaign_id,
                campaign_new_rows,
                total_collected,
            )
            last_info_emit_idx = idx
            last_info_emit_rows = total_collected
        else:
            logger.debug(
                "[%d/%d] Campaign %s bounces fetched: %d rows (cumulative total: %d)",
                idx,
                total_campaigns,
                campaign_id,
                campaign_new_rows,
                total_collected,
            )

    logger.info(
        "Completed bounce extraction: processed %d campaigns, collected %d rows",
        total_campaigns,
        len(bounces),
    )
    return normalize_records(bounces, default_fields)


# =============================================================================
# MEMBERS_ARCHIVED EXTRACTION
# =============================================================================
# This table requires special handling because it needs to:
# 1. Query BigQuery for distinct emails from the members table
# 2. Call /search-members API for each email to find archived status
# 3. Collect and return archived member records

ARCHIVED_EXTRACTION_THREADS = 10
ARCHIVED_REQUESTS_PER_SECOND = 10


class _ArchivedRateLimiter:
    """Thread-safe rate limiter for archived extraction."""

    def __init__(self, interval: float):
        self.interval = interval
        self.lock = threading.Lock()
        self.last_request = 0.0

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_request
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)
            self.last_request = time.time()


class _ArchivedProgressTracker:
    """Thread-safe progress tracker for archived extraction."""

    def __init__(self, total: int, notification_counter: int = 10000):
        self.total = total
        self.notification_counter = notification_counter
        self.processed = 0
        self.archived_found = 0
        self.errors = 0
        self.lock = threading.Lock()
        self.start_time = time.time()

    def update(self, found_archived: int = 0, error: bool = False):
        with self.lock:
            self.processed += 1
            self.archived_found += found_archived
            if error:
                self.errors += 1

    def log_progress(self, force: bool = False):
        with self.lock:
            if force or self.processed % self.notification_counter == 0:
                elapsed = time.time() - self.start_time
                rate = self.processed / elapsed if elapsed > 0 else 0
                remaining = (self.total - self.processed) / rate if rate > 0 else 0
                logger.info(
                    "Archived extraction progress: %d/%d (%.1f%%) | "
                    "Archived: %d | Errors: %d | Rate: %.1f/sec | ETA: %.1f min",
                    self.processed,
                    self.total,
                    self.processed / self.total * 100,
                    self.archived_found,
                    self.errors,
                    rate,
                    remaining / 60,
                )


def _search_member_for_archived(
    email: str,
    session: requests.Session,
    base_url: str,
    rate_limiter: _ArchivedRateLimiter,
    list_names: Dict[str, str],
    max_attempts: int,
    backoff_seconds: float,
) -> List[Dict[str, Any]]:
    """Search for a member and return archived records if found."""
    rate_limiter.wait()

    try:
        response = mailchimp_request(
            session,
            base_url,
            "GET",
            "/search-members",
            params={"query": email},
            timeout=30,
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
        )

        archived_records = []
        full_search = response.get("full_search", {})
        members = full_search.get("members", [])

        for member in members:
            # Only process if email matches exactly (search can return partial matches)
            if member.get("email_address", "").lower() != email.lower():
                continue

            status = member.get("status", "")
            if status == "archived":
                list_id = member.get("list_id", "")
                # Skip if list_names is filtered and this list_id is not in it
                # (test mode filtering)
                if list_names and list_id not in list_names:
                    continue
                archived_records.append(
                    {
                        "list_id": list_id,
                        "email_address": member.get("email_address"),
                        "status": status,
                        "archived_at": member.get("last_changed")
                        or member.get("timestamp_opt"),
                        "tenant_id": list_id,
                        "tenant_name": list_names.get(list_id, list_id),
                        "unique_email_id": member.get("unique_email_id"),
                        "member_id": member.get("id"),
                    }
                )

        return archived_records

    except Exception as e:
        if "404" not in str(e):
            logger.debug("Error searching %s for archived status: %s", email, e)
        return []


def _process_email_for_archived(
    email: str,
    session: requests.Session,
    base_url: str,
    rate_limiter: _ArchivedRateLimiter,
    list_names: Dict[str, str],
    max_attempts: int,
    backoff_seconds: float,
    progress: _ArchivedProgressTracker,
    results: List[Dict[str, Any]],
    results_lock: threading.Lock,
):
    """Process a single email for archived extraction."""
    try:
        archived = _search_member_for_archived(
            email,
            session,
            base_url,
            rate_limiter,
            list_names,
            max_attempts,
            backoff_seconds,
        )

        if archived:
            with results_lock:
                results.extend(archived)

        progress.update(found_archived=len(archived))
        progress.log_progress()

    except Exception as e:
        progress.update(error=True)
        logger.debug("Error processing %s for archived: %s", email, e)


def extract_members_archived(
    session: requests.Session,
    base_url: str,
    bq_client: Any,
    project: str,
    dataset: str,
    list_names: Dict[str, str],
    max_attempts: int = 3,
    backoff_seconds: float = 2.0,
    default_fields: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    notification_counter: int = 10000,
    test_emails: Optional[List[str]] = None,
    test_list_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Extract archived members by checking each known email via /search-members API.

    This function:
    1. Queries BigQuery members table for distinct email addresses
    2. Checks each email via Mailchimp /search-members API (multi-threaded)
    3. Returns records for emails with status="archived" on any list

    Args:
        session: Authenticated Mailchimp requests session
        base_url: Mailchimp API base URL
        bq_client: BigQuery client
        project: GCP project ID
        dataset: BigQuery dataset containing members table
        list_names: Dict mapping list_id -> list_name
        max_attempts: Max retry attempts per API call
        backoff_seconds: Backoff between retries
        default_fields: Default fields to add to each record
        limit: Optional limit on emails to check (for testing)
        notification_counter: Log progress every N emails
        test_emails: Optional list of specific emails to check (bypasses BigQuery query)
        test_list_ids: Optional list of list_ids to filter results to

    Returns:
        List of archived member records
    """
    logger.info("Starting archived members extraction")

    # If test_emails provided, use those instead of querying BigQuery
    if test_emails:
        emails = test_emails
        logger.info("TEST MODE: Using %d provided test emails: %s", len(emails), emails)
    else:
        # Query distinct emails from members table
        members_table = f"{project}.{dataset}.members"
        query = f"""
            SELECT DISTINCT email_address
            FROM `{members_table}`
            WHERE email_address IS NOT NULL
              AND email_address != ''
            ORDER BY email_address
        """
        if limit:
            query += f" LIMIT {limit}"

        logger.info("Querying distinct emails from %s...", members_table)
        try:
            result = bq_client.query(query).result()
            emails = [row.email_address for row in result]
        except Exception as e:
            logger.error("Failed to query members table: %s", e)
            return []

    logger.info("Found %d distinct emails to check", len(emails))

    # If test_list_ids provided, filter list_names to only those lists
    if test_list_ids:
        filtered_list_names = {
            k: v for k, v in list_names.items() if k in test_list_ids
        }
        logger.info(
            "TEST MODE: Filtering to %d lists: %s",
            len(filtered_list_names),
            test_list_ids,
        )
        list_names = filtered_list_names

    if not emails:
        return []

    # Set up multi-threaded extraction
    request_interval = ARCHIVED_EXTRACTION_THREADS / ARCHIVED_REQUESTS_PER_SECOND
    rate_limiter = _ArchivedRateLimiter(interval=request_interval)
    progress = _ArchivedProgressTracker(
        total=len(emails), notification_counter=notification_counter
    )
    results: List[Dict[str, Any]] = []
    results_lock = threading.Lock()

    logger.info(
        "Processing %d emails with %d threads (rate limit: %d req/sec)",
        len(emails),
        ARCHIVED_EXTRACTION_THREADS,
        ARCHIVED_REQUESTS_PER_SECOND,
    )

    with ThreadPoolExecutor(max_workers=ARCHIVED_EXTRACTION_THREADS) as executor:
        futures = [
            executor.submit(
                _process_email_for_archived,
                email,
                session,
                base_url,
                rate_limiter,
                list_names,
                max_attempts,
                backoff_seconds,
                progress,
                results,
                results_lock,
            )
            for email in emails
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.debug("Thread error in archived extraction: %s", e)

    progress.log_progress(force=True)
    logger.info(
        "Archived extraction complete: checked %d emails, found %d archived records",
        progress.processed,
        progress.archived_found,
    )

    # Add default fields to results
    if default_fields:
        for record in results:
            for key, value in default_fields.items():
                if key not in record:
                    record[key] = value

    return results


# =============================================================================
# MEMBERS_ARCHIVED FAST EXTRACTION (NEW - uses direct list query)
# =============================================================================
# This is a MUCH faster approach that queries /lists/{list_id}/members?status=archived
# directly instead of searching each email individually.
#
# Performance comparison:
#   Old approach: O(emails) API calls - 500k emails = 14+ hours
#   New approach: O(lists) API calls - 15 lists = < 1 minute
# =============================================================================


def extract_members_archived_fast(
    session: requests.Session,
    base_url: str,
    lists: List[Dict[str, Any]],
    page_size: int = 1000,
    max_attempts: int = 3,
    backoff_seconds: float = 2.0,
    default_fields: Optional[Dict[str, Any]] = None,
    test_list_ids: Optional[List[str]] = None,
    limit: Optional[int] = None,
    since: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Extract archived members by directly querying each list with status=archived.

    This is MUCH faster than the search-members approach because:
    - O(lists) API calls instead of O(emails)
    - Uses pagination, not individual lookups
    - Leverages existing fetch_paginated_collection infrastructure

    Args:
        session: Authenticated Mailchimp requests session
        base_url: Mailchimp API base URL
        lists: List of list objects (with 'id' and 'name' keys)
        page_size: Number of records per API page
        max_attempts: Max retry attempts per API call
        backoff_seconds: Backoff between retries
        default_fields: Default fields to add to each record
        test_list_ids: Optional list of list_ids to filter to (for testing)
        limit: Optional max number of records to return (for testing)

    Returns:
        List of archived member records
    """
    logger.info("Starting FAST archived members extraction (direct list query)")
    logger.info(
        "This approach queries /lists/{list_id}/members?status=archived directly"
    )
    if since:
        logger.info("Incremental filter: since_last_changed=%s", since)

    if limit:
        logger.info("TEST MODE: Limiting to %d total records", limit)

    archived_members: List[Dict[str, Any]] = []
    lists_processed = 0
    lists_with_archived = 0

    # Filter to test lists if specified
    if test_list_ids:
        lists = [lst for lst in lists if lst.get("id") in test_list_ids]
        logger.info("TEST MODE: Filtering to %d lists: %s", len(lists), test_list_ids)

    total_lists = len(lists)
    logger.info("Processing %d lists for archived members", total_lists)

    for lst in lists:
        list_id = lst.get("id")
        list_name = lst.get("name", list_id)

        if not list_id:
            continue

        lists_processed += 1
        logger.info(
            "[%d/%d] Fetching archived members from list: %s (%s)",
            lists_processed,
            total_lists,
            list_name,
            list_id,
        )

        # Query directly with status=archived
        params: Dict[str, Any] = {"status": "archived"}
        if since:
            params["since_last_changed"] = since
        path = f"/lists/{list_id}/members"

        try:
            records = fetch_paginated_collection(
                session,
                base_url,
                path,
                "members",
                params,
                page_size,
                max_attempts,
                backoff_seconds,
                progress_logger=lambda count, total, lid=list_id, lname=list_name: (
                    logger.debug(
                        "Fetched %d/%s archived members for list %s (%s)",
                        count,
                        total if total else "unknown",
                        lname,
                        lid,
                    )
                ),
                progress_interval=10000,
            )

            if records:
                lists_with_archived += 1

                # Enrich records with list info and standard fields
                for record in records:
                    record["list_id"] = list_id
                    record["tenant_id"] = list_id
                    record["tenant_name"] = list_name
                    # Map last_changed to archived_at for consistency with production schema
                    record["archived_at"] = record.get("last_changed") or record.get(
                        "timestamp_opt"
                    )
                    # Keep the status field (should be 'archived')
                    record.setdefault("status", "archived")

                archived_members.extend(records)
                logger.info(
                    "  Found %d archived members in %s", len(records), list_name
                )

                # Check if we've hit the limit
                if limit and len(archived_members) >= limit:
                    logger.info("  Reached limit of %d records, stopping early", limit)
                    archived_members = archived_members[:limit]
                    break
            else:
                logger.debug("  No archived members in %s", list_name)

        except Exception as e:
            logger.error(
                "Failed to fetch archived members from list %s (%s): %s",
                list_name,
                list_id,
                e,
            )

    logger.info("")
    logger.info("=" * 60)
    logger.info("FAST ARCHIVED EXTRACTION COMPLETE")
    logger.info("=" * 60)
    logger.info("Lists processed: %d", lists_processed)
    logger.info("Lists with archived members: %d", lists_with_archived)
    logger.info("Total archived members found: %d", len(archived_members))
    logger.info("=" * 60)

    # Add default fields to results
    if default_fields:
        for record in archived_members:
            for key, value in default_fields.items():
                if key not in record:
                    record[key] = value

    return normalize_records(archived_members, default_fields)


EXTRACTION_HANDLERS = {
    "campaigns": extract_campaigns,
    "lists": extract_lists,
    "members": extract_members,
    "members_archived": extract_members_archived_fast,
    "unsubscribes": extract_unsubscribes,
    "bounces": extract_bounces,
    "campaign_email_activity": extract_campaign_email_activity,
    "campaign_sent_to": extract_campaign_sent_to,
    "campaign_domain_performance": extract_campaign_domain_performance,
    "campaign_locations": extract_campaign_locations,
    "campaign_open_details": extract_campaign_open_details,
    "campaign_click_details": extract_campaign_click_details,
    "campaign_click_members": extract_campaign_click_members,
    "list_merge_fields": extract_list_merge_fields,
    "list_growth_history": extract_list_growth_history,
    "list_activity": extract_list_activity,
    "list_segments": extract_list_segments,
    "list_segment_members": extract_list_segment_members,
}


def _record_for_standard_path(
    table_files: Dict[str, str],
    table_rows: Dict[str, int],
    logical_table: str,
    *,
    source: str,
    job_id: str,
    bq_client: Any,
    execution_id: Optional[str] = None,
    records: Optional[List[Dict[str, Any]]] = None,
    parquet_path: Optional[str] = None,
    row_count: Optional[int] = None,
    authoritative_schema: Optional[Dict[str, str]] = None,
) -> None:
    """
    Record one table for the standard orchestrator load path.

    Centralizes the contract every converted Mailchimp call site must honor:
      - table_files is keyed by the LOGICAL resource name (the orchestrator looks
        up config.resources[key] for table_name/primary_key/write_disposition);
        an affixed key would miss that lookup.
      - the parquet file must SURVIVE until the orchestrator uploads it (after
        run_pipeline returns). The Batch path's parquet lives under a mkdtemp dir
        that is rmtree'd in run_pipeline's finally, so we COPY it to an
        orchestrator-owned durable temp path.
      - YAML schema auto-update (update_yaml_schema_selective) is preserved here,
        matching execute_full_pipeline, so bypassing it does not silently drop the
        schema write.

    Exactly one of `records` / `parquet_path` must be provided.

    B1: this now DELEGATES to shared/extraction_result.StandardExtractionResult
    (the single shared accumulator), mutating the passed-in table_files/table_rows
    dicts to keep all three call sites and the existing return assembly unchanged.
    The behavior is identical -- stamp metadata before write, durable-copy the
    Batch parquet, preserve the YAML schema auto-update -- because the accumulator
    is a superset of what this helper did.
    """
    from shared.extraction_result import StandardExtractionResult

    acc = StandardExtractionResult(
        source=source, execution_id=execution_id, job_id=job_id, bq_client=bq_client
    )
    if records is not None:
        acc.record_table(
            logical_table,
            records=records,
            row_count=row_count,
            authoritative_schema=authoritative_schema,
        )
    elif parquet_path is not None:
        acc.record_table(
            logical_table,
            parquet_path=parquet_path,
            row_count=row_count,
            authoritative_schema=authoritative_schema,
        )
    else:
        raise ValueError(
            "_record_for_standard_path requires either records or parquet_path"
        )

    # Mirror the accumulator's results into the caller's dicts. A zero-row table
    # records as zero_rows in the accumulator (no table_files entry) -> we leave
    # table_files unset and table_rows at 0, matching the prior behavior.
    table_rows[logical_table] = acc.table_rows.get(logical_table, 0)
    if logical_table in acc.table_files:
        table_files[logical_table] = acc.table_files[logical_table]


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
    export_dir: Optional[str],
    skip_hash_merge: bool,
    archive_staging: bool,
    truncate_staging: bool,
    rebuild: bool,
    bq_client: Any,
    execution_id: str,
    parallel_workers: Optional[int] = None,
    schema_prefix: Optional[str] = None,
    schema_suffix: Optional[str] = None,
    load_pattern: Optional[str] = None,
    groups: Optional[List[str]] = None,
    use_batch_api: Optional[bool] = None,
    limit: Optional[int] = None,
    test_emails: Optional[List[str]] = None,
    test_list_ids: Optional[List[str]] = None,
    **kwargs,
) -> Dict[str, Any]:
    rebuild_mode = rebuild
    del test_mode, batch_size, skip_validation, export_format, export_dir
    del skip_hash_merge, archive_staging, truncate_staging, kwargs

    # Load-path feature flag (Mailchimp-specific). Default 'in_plugin' keeps the
    # legacy execute_full_pipeline behavior. Set pipeline.load_path: orchestrator
    # to extract -> return table_files and let the orchestrator do GCS + external
    # + staging + hash-merge (the standard path every other source uses).
    load_path = (config.get("pipeline", {}) or {}).get("load_path", "in_plugin")
    use_standard_path = load_path == "orchestrator"
    # Standard-path accumulators (logical resource name -> ...). Empty/unused on
    # the in-plugin path.
    std_table_files: Dict[str, str] = {}
    std_table_rows: Dict[str, int] = {}
    if use_standard_path:
        logger.info(
            "Mailchimp load_path=orchestrator: extracting to table_files for the "
            "standard transform path (in-plugin merge disabled)."
        )

    # Handle group/groups parameter (groups takes precedence)
    if groups and not group:
        group = groups[0] if groups else None

    accounts = resolve_mailchimp_accounts(config, sites)
    if not accounts:
        logger.warning("No Mailchimp accounts configured or matched requested sites")
        return {"total_rows": 0, "tables": 0, "status": "no_accounts"}

    extractor_cfg = config.get("extractor", {})
    # Resolve use_batch_api precedence: CLI arg (if provided) > YAML > default True.
    # YAML lives at extractor.use_batch_api in configs/mailchimp.yaml.
    _use_batch_api_input = use_batch_api
    _yaml_use_batch_api = extractor_cfg.get("use_batch_api")
    if use_batch_api is None:
        use_batch_api = bool(extractor_cfg.get("use_batch_api", True))
    logger.info(
        "Batch API resolution: CLI=%s, YAML=%s, FINAL=%s",
        _use_batch_api_input,
        _yaml_use_batch_api,
        use_batch_api,
    )
    page_size = extractor_cfg.get("page_size", 500)
    configured_parallel_workers = extractor_cfg.get("parallel_workers", 1)
    if parallel_workers is not None:
        configured_parallel_workers = parallel_workers
    configured_parallel_workers = max(
        1, min(configured_parallel_workers, MAILCHIMP_MAX_CONCURRENT_REQUESTS)
    )
    # Concurrency for Batch API multi-table extraction. Default to 1 to match
    # the proven daily run behavior; bump in yaml if Mailchimp's per-account
    # batch queue can handle the contention.
    configured_batch_table_workers = max(
        1, int(extractor_cfg.get("parallel_extraction_workers", 1))
    )
    retry_cfg = extractor_cfg.get("retry", {})
    max_attempts = retry_cfg.get("max_attempts", max_retries or 3)
    backoff_seconds = retry_cfg.get("backoff_seconds", 2.0)
    request_timeout = extractor_cfg.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
    rate_limit_cfg = extractor_cfg.get("rate_limit", {})
    requests_per_second = rate_limit_cfg.get("requests_per_second", 9.0)
    configure_mailchimp_rate_limit(requests_per_second)

    if not tables:
        tables = get_available_tables(config, group)
    if not tables:
        logger.warning("No Mailchimp tables requested")
        return {"total_rows": 0, "tables": 0, "status": "no_tables"}

    core_order = ["campaigns", "lists", "members", "unsubscribes"]
    ordered_tables: List[str] = []
    for core_table in core_order:
        if core_table in tables and core_table not in ordered_tables:
            ordered_tables.append(core_table)
    for table_name in tables:
        if table_name not in ordered_tables:
            ordered_tables.append(table_name)
    tables = ordered_tables

    resources_config = config.get("resources", {})
    default_strategy = (extractor_cfg.get("strategy") or "rest").lower()
    valid_strategies = {"rest", "batch", "bulk"}
    table_strategies: Dict[str, str] = {}
    for table in tables:
        table_cfg = resources_config.get(table, {})
        explicit_strategy = table_cfg.get("extraction_strategy")
        table_strategy = (explicit_strategy or default_strategy).lower()
        if table_strategy not in valid_strategies:
            table_strategy = "rest"
        if table in BATCH_ELIGIBLE_TABLES:
            explicit_value = (
                (explicit_strategy or "").lower()
                if isinstance(explicit_strategy, str)
                else ""
            )
            if not use_batch_api:
                table_strategy = "rest"
            elif explicit_value != "rest":
                table_strategy = "batch"
        table_strategies[table] = table_strategy

    bulk_tables_requested = [
        table
        for table in tables
        if table_strategies.get(table) == "bulk" and table in BULK_SUPPORTED_TABLES
    ]
    bulk_table_configs = {
        table: resources_config.get(table, {}) for table in bulk_tables_requested
    }
    bulk_exports_cache: Dict[str, Dict[str, Any]] = {}
    if bulk_tables_requested:
        logger.info(
            "Using Mailchimp account export bulk strategy for tables: %s",
            ", ".join(sorted(bulk_tables_requested)),
        )

    if not bq_client:
        bq_client = get_bigquery_client()

    env = initialize_pipeline_environment(
        config,
        bq_client=bq_client,
        source_default="mailchimp",
        schema_prefix=schema_prefix,
        schema_suffix=schema_suffix,
        execution_id=execution_id,
        rebuild=rebuild_mode,
        require_bucket=True,
        default_ttl=DEFAULT_TTL_DAYS,
    )

    run_execution_id = env.execution_id or execution_id

    metadata_dataset = config.get("pipeline", {}).get(
        "metadata_dataset", "mailchimp_metadata"
    )
    ensure_dataset_exists(bq_client, metadata_dataset)
    metadata_manager = MailchimpMetadataManager(
        bq_client, env.project, metadata_dataset
    )
    window_start_dt, window_end_dt = resolve_run_window(
        refresh_mode, lookback_days, start_date, end_date
    )

    bucket_name = env.bucket_name
    ttl_days = env.ttl_days or DEFAULT_TTL_DAYS

    total_rows = 0
    processed_tables = 0
    table_results: Dict[str, Any] = {}
    table_row_counts: Dict[str, int] = {}
    account_runtime_cache: Dict[str, Dict[str, Any]] = {}

    # Load tenant mapping for multi-tenant support
    tenant_mapping = config.get("source", {}).get("tenant_mapping") or {}

    # Determine effective load pattern
    effective_load_pattern = load_pattern or extractor_cfg.get(
        "default_load_pattern", "standard"
    )

    # Check if multi-table extraction applies
    campaign_tables_requested = [t for t in tables if t in CAMPAIGN_REPORT_TABLES]
    use_multi_table = (
        effective_load_pattern == "multi" and len(campaign_tables_requested) > 1
    )

    # Determine whether to apply campaign filters (full refresh should bypass YAML filters unless explicit range provided)
    apply_campaign_filters = True
    if refresh_mode == "full" and not start_date and not end_date and not lookback_days:
        apply_campaign_filters = False
    campaign_filters_cfg = (
        extractor_cfg.get("campaign_filters") if apply_campaign_filters else None
    )

    account_contexts: Dict[str, Dict[str, Any]] = {}
    for account in accounts:
        context = prepare_account_context(
            account,
            extractor_cfg,
            account_runtime_cache,
            request_timeout,
            metadata_manager,
            window_start_dt,
            window_end_dt,
            campaign_filters_cfg,
            start_date,
            end_date,
            configured_parallel_workers,
            refresh_mode=refresh_mode,
        )
        account_contexts[account["name"]] = context

    if use_multi_table:
        logger.info(
            f"Using multi-table extraction for {len(campaign_tables_requested)} campaign tables"
        )
        logger.info(f"Tables: {', '.join(campaign_tables_requested)}")

        safe_execution_fragment = "".join(
            ch if ch.isalnum() else "_" for ch in (run_execution_id or "run")
        )[:32]
        multi_temp_dir: Optional[str] = None

        try:
            temp_dir_setting = extractor_cfg.get("temp_dir")
            default_temp_root = os.path.join(tempfile.gettempdir(), "mailchimp")
            base_temp_dir = temp_dir_setting or default_temp_root

            try:
                if not os.path.isabs(base_temp_dir):
                    base_temp_dir = os.path.abspath(base_temp_dir)
                os.makedirs(base_temp_dir, exist_ok=True)
                multi_temp_dir = tempfile.mkdtemp(
                    prefix=f"mailchimp_{safe_execution_fragment}_", dir=base_temp_dir
                )
            except Exception as temp_exc:
                logger.warning(
                    "Could not initialize temp directory %s (%s); using system temp",
                    base_temp_dir,
                    temp_exc,
                )
                multi_temp_dir = tempfile.mkdtemp(
                    prefix=f"mailchimp_{safe_execution_fragment}_"
                )

            # Process all accounts for multi-table extraction
            for account in accounts:
                context = account_contexts[account["name"]]
                session = context["session"]
                base_url = context["base_url"]
                account_cache = context["account_cache"]
                default_fields = context["default_fields"]
                lists_cache = context["lists_cache"]
                cache = account_cache

                # Build table configs for multi-table extraction
                table_configs = {
                    t: resources_config.get(t, {}) for t in campaign_tables_requested
                }

                # Call multi-table extractor
                parquet_files_by_table = extract_campaign_multi_table(
                    session,
                    base_url,
                    refresh_mode,
                    lookback_days,
                    start_date,
                    campaign_tables_requested,
                    table_configs,
                    cache,
                    default_fields,
                    tenant_mapping,
                    lists_cache,
                    page_size,
                    max_attempts,
                    backoff_seconds,
                    temp_dir=multi_temp_dir,
                    parallel_workers=configured_parallel_workers,
                    use_batch_api=use_batch_api,
                    execution_id=run_execution_id,
                    batch_table_workers=configured_batch_table_workers,
                    table_strategies=table_strategies,
                )

                # Process Parquet files for each table
                for table_name in campaign_tables_requested:
                    parquet_files = parquet_files_by_table.get(table_name, [])
                    parquet_files = [
                        path for path in parquet_files if path and os.path.exists(path)
                    ]
                    if not parquet_files:
                        logger.warning(f"{table_name}: No data extracted")
                        continue

                    try:
                        table_with_affix = env.format_table(
                            table_name,
                            prefix_separator="_",
                            suffix_separator="_",
                        )

                        effective_schema, yaml_schema, production_schema, _ = (
                            _load_effective_schema(
                                config.get("source", {}).get("name", "mailchimp"),
                                table_name,
                                table_with_affix,
                                bq_client,
                                env,
                            )
                        )

                        casting_schema: Dict[str, str] = (
                            effective_schema or yaml_schema or production_schema or {}
                        )

                        temporary_paths: List[str] = []

                        # Merge multiple Parquet files if needed using DuckDB (no memory overhead)
                        if len(parquet_files) == 1:
                            string_parquet_path = parquet_files[0]
                        else:
                            import duckdb

                            combined_path = os.path.join(
                                multi_temp_dir,
                                f"{table_name}_{uuid.uuid4().hex}_combined.parquet",
                            )
                            temporary_paths.append(combined_path)
                            file_list = ", ".join([f"'{f}'" for f in parquet_files])

                            conn = duckdb.connect(database=":memory:")
                            try:
                                conn.execute("PRAGMA memory_limit='4GB'")
                                conn.execute(
                                    f"""
                                    COPY (
                                        SELECT * FROM read_parquet([{file_list}])
                                    ) TO '{combined_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
                                    """
                                )
                            finally:
                                conn.close()

                            string_parquet_path = combined_path
                            logger.info(
                                f"{table_name}: Merged {len(parquet_files)} Parquet files"
                            )

                        parquet_path = string_parquet_path
                        row_count: int

                        if casting_schema:
                            import duckdb

                            typed_parquet_path = os.path.join(
                                multi_temp_dir,
                                f"{table_name}_{uuid.uuid4().hex}_typed.parquet",
                            )
                            temporary_paths.append(typed_parquet_path)

                            conn = duckdb.connect(database=":memory:")
                            try:
                                conn.execute("PRAGMA memory_limit='4GB'")
                                schema_query = f"DESCRIBE SELECT * FROM read_parquet('{string_parquet_path}')"
                                schema_df = conn.execute(schema_query).fetchdf()
                                source_columns = schema_df["column_name"].tolist()
                                source_column_set = set(source_columns)

                                target_order: List[str] = []
                                for col in source_columns:
                                    if col not in target_order:
                                        target_order.append(col)

                                for col in casting_schema.keys():
                                    if col not in target_order:
                                        target_order.append(col)

                                if default_fields:
                                    for col in default_fields.keys():
                                        if col not in target_order:
                                            target_order.append(col)

                                cast_expressions: List[str] = []
                                missing_with_defaults: List[str] = []
                                missing_without_defaults: List[str] = []

                                for col in target_order:
                                    target_type = casting_schema.get(col, "STRING")

                                    if col in source_column_set:
                                        source_expr = f'"{col}"'
                                    else:
                                        literal_value = None
                                        if default_fields:
                                            literal_value = default_fields.get(col)
                                        if literal_value is None:
                                            source_expr = "NULL"
                                            missing_without_defaults.append(col)
                                        else:
                                            source_expr = _format_duckdb_literal(
                                                literal_value
                                            )
                                            missing_with_defaults.append(col)

                                    cast_expressions.append(
                                        _build_duckdb_cast(
                                            col, source_expr, target_type
                                        )
                                    )

                                if missing_with_defaults:
                                    logger.debug(
                                        "%s: Injected default values for columns missing from source parquet: %s",
                                        table_name,
                                        ", ".join(missing_with_defaults),
                                    )

                                if missing_without_defaults:
                                    logger.debug(
                                        "%s: Columns missing from source parquet; populating NULLs: %s",
                                        table_name,
                                        ", ".join(missing_without_defaults),
                                    )

                                select_clause = (
                                    ",\n                ".join(cast_expressions)
                                    if cast_expressions
                                    else "*"
                                )

                                conn.execute(
                                    f"""
                                    COPY (
                                        SELECT
                                        {select_clause}
                                        FROM read_parquet('{string_parquet_path}')
                                    ) TO '{typed_parquet_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
                                    """
                                )

                                row_count = conn.execute(
                                    f"SELECT COUNT(*) FROM read_parquet('{typed_parquet_path}')"
                                ).fetchone()[0]
                            finally:
                                conn.close()

                            parquet_path = typed_parquet_path
                            logger.info(
                                f"{table_name}: Rewrote Parquet with {len(casting_schema)} typed columns from schema cache"
                            )
                        else:
                            import duckdb

                            conn = duckdb.connect(database=":memory:")
                            try:
                                row_count = conn.execute(
                                    f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
                                ).fetchone()[0]
                            finally:
                                conn.close()

                            logger.warning(
                                f"{table_name}: No authoritative schema found - using all-STRING Parquet"
                            )

                        if row_count == 0:
                            logger.warning(
                                f"{table_name}: No records found in Parquet files"
                            )
                            continue

                        logger.info(
                            f"{table_name}: {row_count:,} rows in prepared Parquet"
                        )

                        discovered_schema: Dict[str, str] = {}
                        try:
                            with pq.ParquetFile(parquet_path) as parquet_file:
                                arrow_schema = parquet_file.schema_arrow
                                discovered_schema = {
                                    field.name: _arrow_type_to_bq(field.type)
                                    for field in arrow_schema
                                }
                        except Exception as exc:
                            logger.warning(
                                "%s: Unable to derive schema from %s: %s",
                                table_name,
                                parquet_path,
                                exc,
                            )
                            discovered_schema = {}

                        table_config = resources_config.get(table_name, {})
                        primary_keys = table_config.get("primary_key", [])
                        if isinstance(primary_keys, str):
                            primary_keys = [primary_keys]

                        if not use_standard_path:
                            drop_production_table_if_needed(
                                env, bq_client, table_with_affix
                            )
                        job_id_base = (
                            run_execution_id or f"mailchimp_{table_with_affix}"
                        )
                        job_id = f"{job_id_base}_{uuid.uuid4().hex[:8]}"

                        # merge_cfg is only consumed by the in-plugin execute_full_pipeline
                        # call below; the standard path lets the orchestrator build it.
                        merge_cfg = {
                            "source": config.get("source", {}).get("name", "mailchimp"),
                            "execution_id": run_execution_id or job_id,
                            "rebuild": bool(rebuild_mode),
                            "refresh_mode": refresh_mode,  # CRITICAL: Controls DELETE behavior
                        }

                        authoritative_schema = dict(casting_schema)
                        if discovered_schema:
                            new_columns = {
                                col
                                for col in discovered_schema
                                if col not in authoritative_schema
                            }
                            force_string_fields = table_config.get(
                                "force_string_fields", []
                            )
                            inferred_types = _infer_types_for_new_columns_from_parquet(
                                parquet_path,
                                new_columns,
                                table_name,
                                force_string_fields=force_string_fields,
                            )

                            for column in new_columns:
                                inferred_type = inferred_types.get(column)
                                authoritative_schema[column] = (
                                    inferred_type
                                    or discovered_schema.get(column)
                                    or "STRING"
                                )

                            for column, col_type in discovered_schema.items():
                                authoritative_schema.setdefault(
                                    column, col_type or "STRING"
                                )

                        if default_fields:
                            for column in default_fields.keys():
                                authoritative_schema.setdefault(
                                    column,
                                    casting_schema.get(column, "STRING") or "STRING",
                                )

                        if use_standard_path:
                            # Standard path: copy the combined Batch parquet to a
                            # durable location and record it by LOGICAL name; the
                            # orchestrator does GCS + external + merge.
                            _record_for_standard_path(
                                std_table_files,
                                std_table_rows,
                                table_name,
                                source="mailchimp",
                                job_id=job_id,
                                bq_client=bq_client,
                                parquet_path=parquet_path,
                                row_count=row_count,
                                authoritative_schema=(
                                    authoritative_schema
                                    if authoritative_schema
                                    else None
                                ),
                            )
                        else:
                            pipeline_stats = execute_full_pipeline(
                                None,
                                config.get("source", {}).get("name", "mailchimp"),
                                table_with_affix,
                                bq_client,
                                env.project,
                                bucket_name,
                                job_id,
                                ttl_days=ttl_days,
                                cleanup_local=True,
                                staging_dataset=env.staging_dataset,
                                production_dataset=env.production_dataset,
                                primary_keys=primary_keys,
                                merge_config=merge_cfg,
                                prebuilt_parquet_path=parquet_path,
                                row_count_override=row_count,
                                base_table_name=table_name,
                                authoritative_schema=(
                                    authoritative_schema
                                    if authoritative_schema
                                    else None
                                ),
                            )
                            table_results[table_with_affix] = pipeline_stats

                        processed_tables += 1
                        table_row_counts[table_with_affix] = row_count
                        total_rows += row_count
                        logger.info(
                            f"{table_with_affix}: Pipeline complete - {row_count:,} rows"
                        )

                        # Cleanup temporary Parquet files
                        for parquet_file in parquet_files:
                            try:
                                os.remove(parquet_file)
                            except Exception:
                                pass
                        for temp_path in temporary_paths:
                            try:
                                if temp_path and os.path.exists(temp_path):
                                    os.remove(temp_path)
                            except Exception:
                                pass

                    except Exception as exc:
                        logger.error(f"{table_name}: Failed to process: {exc}")

        finally:
            if multi_temp_dir:
                try:
                    shutil.rmtree(multi_temp_dir, ignore_errors=True)
                except Exception as cleanup_exc:
                    logger.debug(
                        "Failed to clean up temp directory %s: %s",
                        multi_temp_dir,
                        cleanup_exc,
                    )

        # Remove campaign tables from the main loop (they've been processed)
        tables = [t for t in tables if t not in campaign_tables_requested]

    # ==========================================================================
    # SPECIAL HANDLING: members_archived table
    # Uses FAST extraction via direct /lists/{list_id}/members?status=archived
    # This is O(lists) instead of O(emails) - typically 1000x+ faster
    # ==========================================================================
    if "members_archived" in tables:
        logger.info("")
        logger.info("=" * 70)
        logger.info("MEMBERS_ARCHIVED: Starting FAST extraction")
        logger.info("=" * 70)

        table_config = dict(resources_config.get("members_archived", {}))
        if not table_config.get("active", True):
            logger.info("Table 'members_archived' is marked as active: false, skipping")
        else:
            # Build list of all lists from all accounts
            all_lists: List[Dict[str, Any]] = []
            seen_list_ids: Set[str] = set()

            logger.info("Collecting lists from %d account(s)...", len(accounts))
            for account in accounts:
                account_name = account["name"]
                context = account_contexts.get(account_name)
                if not context:
                    logger.warning(
                        "  No context for account '%s', skipping", account_name
                    )
                    continue

                lists_cache = context.get("lists_cache", {})
                logger.info(
                    "  Account '%s': found %d lists in cache",
                    account_name,
                    len(lists_cache),
                )

                for list_id, list_data in lists_cache.items():
                    if list_id in seen_list_ids:
                        continue
                    seen_list_ids.add(list_id)

                    if isinstance(list_data, dict):
                        list_name = list_data.get("name", list_id)
                        all_lists.append({"id": list_id, "name": list_name})
                    else:
                        all_lists.append({"id": list_id, "name": str(list_data)})

            logger.info("Total unique lists to process: %d", len(all_lists))
            if all_lists:
                for i, lst in enumerate(all_lists[:5]):  # Show first 5
                    logger.info("  [%d] %s (%s)", i + 1, lst.get("name"), lst.get("id"))
                if len(all_lists) > 5:
                    logger.info("  ... and %d more", len(all_lists) - 5)

            if not all_lists:
                logger.warning("No lists found to process for members_archived!")
                logger.warning("This could mean:")
                logger.warning("  - No accounts configured")
                logger.warning("  - Lists API returned empty")
                logger.warning("  - Check the 'accounts' section in mailchimp.yaml")
            else:
                # Use first account's session
                first_account = accounts[0]
                context = account_contexts[first_account["name"]]
                session = context["session"]
                base_url = context["base_url"]
                default_fields = dict(context["default_fields"])

                logger.info("")
                logger.info("Starting API extraction...")

                # Compute incremental since timestamp for archived members
                archived_since = compute_since_timestamp(
                    refresh_mode, lookback_days, start_date, table_config
                )
                if archived_since:
                    logger.info(
                        "Incremental mode: fetching archived members changed since %s",
                        archived_since,
                    )
                else:
                    logger.info("Full refresh: fetching all archived members")

                # Extract archived members using FAST direct list query
                # This queries /lists/{list_id}/members?status=archived for each list
                # instead of searching each email individually
                archived_records = extract_members_archived_fast(
                    session=session,
                    base_url=base_url,
                    lists=all_lists,
                    page_size=page_size,
                    max_attempts=max_attempts,
                    backoff_seconds=backoff_seconds,
                    default_fields=default_fields,
                    test_list_ids=test_list_ids,
                    limit=limit,
                    since=archived_since,
                )

                if archived_records:
                    logger.info(
                        "Extracted %d archived member records", len(archived_records)
                    )

                    # Add execution metadata
                    set_execution_metadata(
                        archived_records,
                        run_execution_id or execution_id,
                        source="mailchimp",
                    )

                    table_with_affix = env.format_table(
                        "members_archived",
                        prefix_separator="_",
                        suffix_separator="_",
                    )

                    table_row_counts["members_archived"] = len(archived_records)
                    primary_keys = table_config.get("primary_key") or [
                        "list_id",
                        "email_address",
                    ]
                    if isinstance(primary_keys, str):
                        primary_keys = [primary_keys]

                    if not use_standard_path:
                        drop_production_table_if_needed(
                            env, bq_client, table_with_affix
                        )
                    job_id = f"mailchimp_members_archived_{uuid.uuid4().hex[:8]}"

                    if use_standard_path:
                        # members_archived is an incremental upsert (7-day lookback)
                        # that accumulates archived members over time -- NOT a full
                        # replace despite the legacy comments. It converts as a plain
                        # hash-merge table like the REST/BULK tables.
                        _record_for_standard_path(
                            std_table_files,
                            std_table_rows,
                            "members_archived",
                            source="mailchimp",
                            job_id=job_id,
                            bq_client=bq_client,
                            execution_id=run_execution_id,
                            records=archived_records,
                            row_count=len(archived_records),
                        )
                        pipeline_stats = None
                    else:
                        # In-plugin path only: the merge_cfg below drives the legacy
                        # behavior. NOTE: "rebuild=True/refresh_mode=full" here is
                        # inert for delete/truncate -- process_hash_merge keys DELETE
                        # off refresh_mode=="rebuild", so this is an UPSERT, not a
                        # replace. (The standard branch above is equivalent.)
                        merge_cfg = {
                            "source": config.get("source", {}).get("name", "mailchimp"),
                            "execution_id": run_execution_id or job_id,
                            "rebuild": True,
                            "refresh_mode": "full",
                        }
                        pipeline_stats = execute_full_pipeline(
                            archived_records,
                            config.get("source", {}).get("name", "mailchimp"),
                            table_with_affix,
                            bq_client,
                            env.project,
                            bucket_name,
                            job_id,
                            ttl_days=ttl_days,
                            cleanup_local=True,
                            staging_dataset=env.staging_dataset,
                            production_dataset=env.production_dataset,
                            primary_keys=primary_keys,
                            merge_config=merge_cfg,
                            base_table_name="members_archived",
                        )

                    total_rows += len(archived_records)
                    processed_tables += 1
                    table_results["members_archived"] = {
                        "rows": len(archived_records),
                        "status": "success",
                        "pipeline_stats": pipeline_stats,
                    }
                else:
                    logger.info("No archived members found")
                    table_results["members_archived"] = {"rows": 0, "status": "success"}

        # Remove from main loop
        tables = [t for t in tables if t != "members_archived"]

    for table in tables:
        handler = EXTRACTION_HANDLERS.get(table)
        if not handler:
            logger.warning("No handler configured for mailchimp table %s", table)
            continue

        # Check if table is active before processing
        table_config = dict(resources_config.get(table, {}))
        if not table_config.get("active", True):
            logger.info("Table '%s' is marked as active: false, skipping", table)
            continue

        logger.info("\n\n=== Extracting mailchimp table: %s ===", table)
        if "batch" not in table_config and extractor_cfg.get("batch"):
            table_config["batch"] = extractor_cfg["batch"]
        table_strategy = table_strategies.get(table, "rest")
        if table_strategy == "bulk" and table not in BULK_SUPPORTED_TABLES:
            logger.warning(
                "Table %s requested bulk strategy but is unsupported; falling back to REST",
                table,
            )
            table_strategy = "rest"

        merged_records: List[Dict[str, Any]] = []

        global_lists_cache: Dict[str, Dict[str, Any]] = {}
        for account in accounts:
            context = account_contexts[account["name"]]
            session = context["session"]
            base_url = context["base_url"]
            account_cache = context["account_cache"]
            lists_cache = context["lists_cache"]
            account_label = context["account_label"]

            global_lists_cache.update(lists_cache)

            default_fields = dict(context["default_fields"])
            if account.get("extra_fields"):
                default_fields.update(account["extra_fields"])

            cache: Dict[str, Any] = account_cache
            if account_cache.get("preloaded_lists"):
                cache["lists_raw"] = account_cache["preloaded_lists"]
            else:
                cache.pop("lists_raw", None)
            cache.pop("campaigns_raw_records", None)
            cache.pop("lists_raw_records", None)
            account_records: List[Dict[str, Any]] = []
            use_bulk = table_strategy == "bulk" and table in bulk_tables_requested

            if use_bulk:
                cache_entry = bulk_exports_cache.get(account["name"])
                if cache_entry and "error" in cache_entry:
                    use_bulk = False
                elif cache_entry and "data" in cache_entry:
                    account_records = cache_entry["data"].get(table, []) or []
                else:
                    try:
                        logger.debug(
                            "Starting Mailchimp bulk export for %s (account %s)",
                            table,
                            account["name"],
                        )
                        parsed = extract_bulk_events_for_account(
                            session,
                            base_url,
                            {
                                tbl: bulk_table_configs.get(tbl, {})
                                for tbl in bulk_tables_requested
                            },
                            refresh_mode,
                            lookback_days,
                            start_date,
                            max_attempts,
                            backoff_seconds,
                        )
                        normalized_map = {
                            bulk_table: normalize_records(rows, default_fields)
                            for bulk_table, rows in parsed.items()
                        }
                        bulk_exports_cache[account["name"]] = {"data": normalized_map}
                        account_records = normalized_map.get(table, []) or []
                        logger.debug(
                            "Completed Mailchimp bulk export for %s (account %s)",
                            table,
                            account["name"],
                        )
                    except Exception as bulk_exc:
                        bulk_exports_cache[account["name"]] = {"error": str(bulk_exc)}
                        logger.warning(
                            "Bulk export failed for table %s (account %s): %s. Falling back to REST",
                            table,
                            account["name"],
                            bulk_exc,
                        )
                        use_bulk = False

            if not use_bulk:
                handler_strategy = "batch" if table_strategy == "batch" else "rest"
                try:
                    account_records = handler(
                        session,
                        base_url,
                        refresh_mode,
                        lookback_days,
                        start_date,
                        table_config,
                        page_size,
                        max_attempts,
                        backoff_seconds,
                        cache,
                        default_fields,
                        strategy=handler_strategy,
                    )
                except RuntimeError as exc:
                    if handler_strategy == "batch":
                        logger.warning(
                            "Batch extraction failed for %s (account %s): %s; retrying with REST",
                            table,
                            account["name"],
                            exc,
                        )
                        try:
                            account_records = handler(
                                session,
                                base_url,
                                refresh_mode,
                                lookback_days,
                                start_date,
                                table_config,
                                page_size,
                                max_attempts,
                                backoff_seconds,
                                cache,
                                default_fields,
                                strategy="rest",
                            )
                            handler_strategy = "rest"
                        except Exception as rest_exc:
                            logger.error(
                                "REST fallback failed for %s (account %s): %s",
                                table,
                                account["name"],
                                rest_exc,
                            )
                            continue
                    else:
                        lowered = str(exc).lower()
                        if (
                            "429" in lowered
                            or "rate limit" in lowered
                            or "too many requests" in lowered
                        ):
                            logger.error(
                                "Mailchimp rate limit hit while extracting %s for account %s: %s",
                                table,
                                account["name"],
                                exc,
                            )
                        else:
                            logger.error(
                                "Mailchimp extraction failed for %s (account %s): %s",
                                table,
                                account["name"],
                                exc,
                            )
                        raise
                except Exception as exc:
                    import traceback

                    logger.error(
                        "Unexpected Mailchimp error for %s (account %s): %r\n%s",
                        table,
                        account["name"],
                        exc,
                        traceback.format_exc(),
                    )
                    continue

            fetched_count = len(account_records) if account_records else 0
            logger.info(
                "Account %s returned %d %s records",
                account["name"],
                fetched_count,
                table,
            )
            if account_records:
                merged_records.extend(account_records)

            if table == "campaigns":
                raw_campaigns = cache.get("campaigns_raw_records") or []
                if raw_campaigns:
                    merged_campaigns = merge_records_by_key(
                        account_cache.get("preloaded_campaigns", []),
                        raw_campaigns,
                        "id",
                    )
                    account_cache["preloaded_campaigns"] = merged_campaigns
                    cache["preloaded_campaigns"] = merged_campaigns
                    metadata_manager.upsert_campaigns(account["name"], raw_campaigns)
                    cache.pop("campaigns_raw_records", None)
            elif table == "lists":
                raw_lists = (
                    cache.get("lists_raw_records") or cache.get("lists_raw") or []
                )
                if raw_lists:
                    merged_lists = merge_records_by_key(
                        account_cache.get("preloaded_lists", []), raw_lists, "id"
                    )
                    account_cache["preloaded_lists"] = merged_lists
                    cache["preloaded_lists"] = merged_lists
                    metadata_manager.upsert_lists(account["name"], raw_lists)
                    cache.pop("lists_raw_records", None)
                    if merged_lists:
                        cache["lists_raw"] = merged_lists
                    else:
                        cache.pop("lists_raw", None)
                    refreshed_lists_cache: Dict[str, Dict[str, Any]] = {}
                    for lst in merged_lists:
                        list_id = lst.get("id")
                        if not list_id:
                            continue
                        refreshed_lists_cache[list_id] = {
                            "name": lst.get("name", list_id)
                        }
                    account_cache["lists_cache"] = refreshed_lists_cache
                    global_lists_cache.update(refreshed_lists_cache)

        if not merged_records:
            logger.info("No records extracted for mailchimp table %s", table)
            continue

        # Special handling for tables where list_id is not a top-level field
        # The enrich_record_with_tenant_info function expects 'list_id' at top level
        if table == "lists":
            # lists table: the record's 'id' IS the list_id
            for record in merged_records:
                record["list_id"] = record.get("id")
        elif table == "campaigns":
            # campaigns table: list_id is nested inside recipients.list_id
            for record in merged_records:
                recipients = record.get("recipients")
                if isinstance(recipients, dict) and recipients.get("list_id"):
                    record["list_id"] = recipients.get("list_id")
                elif isinstance(recipients, str):
                    # recipients might be JSON string
                    try:
                        import json

                        recipients_dict = json.loads(recipients)
                        if recipients_dict.get("list_id"):
                            record["list_id"] = recipients_dict.get("list_id")
                    except (json.JSONDecodeError, TypeError):
                        pass

        # Enrich all records with tenant information
        for record in merged_records:
            enrich_record_with_tenant_info(record, tenant_mapping, global_lists_cache)

        logger.info("Total records aggregated for %s: %d", table, len(merged_records))

        table_with_affix = env.format_table(
            table,
            prefix_separator="_",
            suffix_separator="_",
        )

        effective_schema, _, _, _ = _load_effective_schema(
            config.get("source", {}).get("name", "mailchimp"),
            table,
            table_with_affix,
            bq_client,
            env,
        )

        authoritative_schema = dict(effective_schema) if effective_schema else {}

        # Get force_string_fields BEFORE inference so it affects discovery
        force_string_fields = table_config.get("force_string_fields", [])

        inferred_new_types = _infer_types_for_new_columns_from_records(
            merged_records,
            authoritative_schema,
            table,
            force_string_fields=force_string_fields,
        )
        if inferred_new_types:
            authoritative_schema.update(inferred_new_types)

        # Apply force_string_fields override again as final safety check
        # This catches any fields that may have been added through other paths
        if force_string_fields:
            for field in force_string_fields:
                if field in authoritative_schema:
                    authoritative_schema[field] = "STRING"
                    logger.debug(
                        f"Forced {field} to STRING type (post-inference safety check)"
                    )

        if authoritative_schema:
            _coerce_records_to_schema(merged_records, authoritative_schema)

        table_row_counts[table] = len(merged_records)
        primary_keys = table_config.get("primary_key") or ["id"]
        if isinstance(primary_keys, str):
            primary_keys = [primary_keys]
        if not use_standard_path:
            drop_production_table_if_needed(env, bq_client, table_with_affix)
        job_id_base = run_execution_id or f"mailchimp_{table_with_affix}"
        job_id = f"{job_id_base}_{uuid.uuid4().hex[:8]}"
        if use_standard_path:
            _record_for_standard_path(
                std_table_files,
                std_table_rows,
                table,
                source="mailchimp",
                job_id=job_id,
                bq_client=bq_client,
                execution_id=run_execution_id,
                records=merged_records,
                row_count=len(merged_records),
                authoritative_schema=(
                    authoritative_schema if authoritative_schema else None
                ),
            )
        else:
            merge_cfg = {
                "source": config.get("source", {}).get("name", "mailchimp"),
                "execution_id": run_execution_id or job_id,
                "rebuild": bool(rebuild_mode),
                "refresh_mode": refresh_mode,  # CRITICAL: Controls DELETE behavior
            }
            pipeline_stats = execute_full_pipeline(
                merged_records,
                config.get("source", {}).get("name", "mailchimp"),
                table_with_affix,
                bq_client,
                env.project,
                bucket_name,
                job_id,
                ttl_days=ttl_days,
                cleanup_local=True,
                staging_dataset=env.staging_dataset,
                production_dataset=env.production_dataset,
                primary_keys=primary_keys,
                merge_config=merge_cfg,
                base_table_name=table,
                authoritative_schema=(
                    authoritative_schema if authoritative_schema else None
                ),
            )
            table_results[table_with_affix] = pipeline_stats
        processed_tables += 1
        total_rows += len(merged_records)

    if use_standard_path:
        # Standard path: hand the orchestrator table_files (logical key -> durable
        # parquet path) and let it do GCS + external + staging + hash-merge.
        return {
            "total_rows": total_rows,
            "tables": processed_tables,
            "status": "completed" if processed_tables else "no_data",
            "table_files": std_table_files,
            "table_rows": std_table_rows,
        }

    return {
        "total_rows": total_rows,
        "tables": processed_tables,
        "status": "completed" if processed_tables else "no_data",
        "details": table_results,
        "table_rows": table_row_counts,
    }


def get_available_sites(config: Dict[str, Any]) -> List[str]:
    return config.get("source", {}).get("sites", [])


def prepare_account_context(
    account: Dict[str, Any],
    extractor_cfg: Dict[str, Any],
    account_runtime_cache: Dict[str, Dict[str, Any]],
    request_timeout: int,
    metadata_manager: MailchimpMetadataManager,
    window_start_dt: Optional[datetime],
    window_end_dt: Optional[datetime],
    campaign_filters_cfg: Optional[Dict[str, Any]],
    start_date: Optional[str],
    end_date: Optional[str],
    configured_parallel_workers: int,
    refresh_mode: str = "incremental",
) -> Dict[str, Any]:
    account_cache = account_runtime_cache.setdefault(account["name"], {})
    session = create_mailchimp_session(account["api_key"], request_timeout)
    base_url = f"https://{account['server_prefix']}.api.mailchimp.com/3.0"

    # Skip metadata preloading for full refresh - will fetch fresh from API
    # This avoids loading 100K+ campaigns from BigQuery which takes forever
    if refresh_mode == "full":
        logger.info(
            "Full refresh: skipping metadata preload (will fetch fresh from API)"
        )
        account_cache["preloaded_campaigns"] = []
        account_cache["preloaded_lists"] = []
    else:
        if "preloaded_campaigns" not in account_cache:
            account_cache["preloaded_campaigns"] = metadata_manager.load_campaigns(
                account["name"], window_start_dt, window_end_dt
            )
        if "preloaded_lists" not in account_cache:
            account_cache["preloaded_lists"] = metadata_manager.load_lists(
                account["name"]
            )

    account_metadata = fetch_account_metadata(session, base_url, account_cache)
    account_label = (
        account.get("name")
        or account_metadata.get("account_name")
        or account["server_prefix"]
    )

    lists_metadata = account_cache.get("preloaded_lists") or []
    if not lists_metadata:
        try:
            lists_payload = mailchimp_request(
                session, base_url, "GET", "/lists", {"count": 1000}, 3, 2.0
            )
            lists_metadata = (
                lists_payload.get("lists", [])
                if isinstance(lists_payload, dict)
                else []
            )
        except Exception as exc:
            logger.warning(
                "Could not fetch lists for account %s: %s", account["name"], exc
            )
            lists_metadata = []
        if lists_metadata:
            account_cache["preloaded_lists"] = lists_metadata

    lists_cache: Dict[str, Dict[str, Any]] = {}
    for lst in lists_metadata:
        list_id = lst.get("id")
        if list_id:
            lists_cache[list_id] = {"name": lst.get("name", list_id)}
    account_cache["lists_cache"] = lists_cache

    default_fields = {
        "source_account": account_label,
        "mailchimp_account_name": account_metadata.get("account_name") or account_label,
        "mailchimp_account_id": account_metadata.get("account_id")
        or account["server_prefix"],
        "mailchimp_server_prefix": account["server_prefix"],
    }

    cache = account_cache
    cache["extractor"] = extractor_cfg
    cache["api_key"] = account["api_key"]
    cache["timeout_seconds"] = request_timeout
    cache["preloaded_campaigns"] = account_cache.get("preloaded_campaigns")
    cache["preloaded_lists"] = account_cache.get("preloaded_lists")
    if account_cache.get("preloaded_lists"):
        cache["lists_raw"] = account_cache["preloaded_lists"]
    cache.pop("campaigns_raw_records", None)
    cache.pop("lists_raw_records", None)
    if campaign_filters_cfg:
        cache["campaign_filters"] = campaign_filters_cfg
    else:
        cache.pop("campaign_filters", None)
    if configured_parallel_workers > 1:
        cache["parallel_workers"] = min(
            configured_parallel_workers, MAILCHIMP_MAX_CONCURRENT_REQUESTS
        )
    else:
        cache.pop("parallel_workers", None)
    if start_date:
        cache["explicit_start_date"] = start_date
    else:
        cache.pop("explicit_start_date", None)
    if end_date:
        cache["explicit_end_date"] = end_date
    else:
        cache.pop("explicit_end_date", None)

    return {
        "session": session,
        "base_url": base_url,
        "account_cache": cache,
        "account_metadata": account_metadata,
        "account_label": account_label,
        "default_fields": default_fields,
        "lists_cache": lists_cache,
        "lists_metadata": lists_metadata,
    }


def _process_campaign_all_tables(
    campaign: Dict[str, Any],
    requested_tables: List[str],
    session: Optional[requests.Session],
    base_url: str,
    since: Optional[str],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    default_fields: Optional[Dict[str, Any]],
    temp_dir: str,
    applicable_tables: Optional[Set[str]] = None,
    api_key: Optional[str] = None,
    request_timeout: int = DEFAULT_TIMEOUT_SECONDS,
    execution_id: Optional[str] = None,
) -> Dict[str, int]:
    """
    Process a single campaign and extract all requested table data.
    Thread-safe worker function for multi-table extraction.

    Returns dict of table_name -> row_count written to local files
    """
    campaign_id = campaign.get("id")
    if not campaign_id:
        return {}

    list_id = (campaign.get("recipients") or {}).get("list_id")
    tables_to_process: Set[str] = (
        set(applicable_tables) if applicable_tables else set(requested_tables)
    )
    if not tables_to_process:
        return {}

    row_counts: Dict[str, int] = {table: 0 for table in tables_to_process}

    worker_session = session
    if worker_session is None:
        if not api_key:
            raise ValueError("API key required for worker session")
        worker_session = get_worker_mailchimp_session(api_key, request_timeout)

    since_dt = parse_mailchimp_timestamp(since) if since else None
    click_urls_cache: Optional[List[Dict[str, Any]]] = None

    for table in tables_to_process:
        rows: List[Dict[str, Any]] = []

        try:
            if table == "campaign_sent_to":
                wait_if_needed(MAILCHIMP_RATE_LIMITER)
                params = _with_fields(
                    {"since": since} if since else None, "campaign_sent_to"
                )
                recipients = fetch_paginated_collection(
                    worker_session,
                    base_url,
                    f"/reports/{campaign_id}/sent-to",
                    "sent_to",
                    params or {},
                    page_size,
                    max_attempts,
                    backoff_seconds,
                )
                for entry in recipients:
                    row = dict(entry)
                    row["campaign_id"] = campaign_id
                    row.setdefault("list_id", list_id)
                    if "subscriber_hash" not in row and row.get("email_address"):
                        row["subscriber_hash"] = derive_email_id(
                            campaign_id, row["email_address"]
                        )
                    if default_fields:
                        for key, value in default_fields.items():
                            row.setdefault(key, value)
                    rows.append(row)

            elif table == "campaign_email_activity":
                wait_if_needed(MAILCHIMP_RATE_LIMITER)
                # page_size is honored directly here. The old min(page_size, 200)
                # cap had no recorded rationale and cost 5x the requests on the
                # largest table: /email-activity accepts count=1000 and returns
                # byte-identical data (verified -- 3,150 members and 1,562
                # activities either way), while a 3,150-recipient campaign drops
                # from 16 pages/5.0s to 4 pages/1.7s.
                activity_page_size = page_size
                params = {"count": activity_page_size}
                if since:
                    params["since"] = since
                params = _with_fields(params, "campaign_email_activity")
                emails = fetch_paginated_collection(
                    worker_session,
                    base_url,
                    f"/reports/{campaign_id}/email-activity",
                    "emails",
                    params,
                    activity_page_size,
                    max_attempts,
                    backoff_seconds,
                )
                for email_entry in emails or []:
                    email_id = email_entry.get("email_id") or derive_email_id(
                        campaign_id, email_entry.get("email_address")
                    )
                    email_address = email_entry.get("email_address")
                    activities = email_entry.get("activity") or []
                    for activity_entry in activities:
                        action = activity_entry.get("action") or activity_entry.get(
                            "type"
                        )
                        timestamp = activity_entry.get("timestamp")
                        activity_dt = (
                            parse_mailchimp_timestamp(timestamp) if timestamp else None
                        )
                        if since_dt and activity_dt and activity_dt < since_dt:
                            continue
                        if since_dt and activity_dt is None:
                            continue
                        row = {
                            "campaign_id": campaign_id,
                            "list_id": list_id,
                            "email_id": email_id,
                            "email_address": email_address,
                            "action": action,
                            "activity_timestamp": activity_dt,
                            "activity_type": activity_entry.get("type"),
                            "ip": activity_entry.get("ip"),
                            "url": activity_entry.get("url"),
                            "device": activity_entry.get("device"),
                            "user_agent": activity_entry.get("user_agent"),
                        }
                        geo_info = activity_entry.get("geo")
                        if isinstance(geo_info, dict):
                            row["geo_country"] = geo_info.get("country")
                            row["geo_region"] = geo_info.get("region")
                        else:
                            row["geo_country"] = activity_entry.get("country")
                            row["geo_region"] = activity_entry.get("region")
                        if default_fields:
                            for key, value in default_fields.items():
                                row.setdefault(key, value)
                        rows.append(row)

            elif table == "campaign_open_details":
                wait_if_needed(MAILCHIMP_RATE_LIMITER)
                params = _with_fields(
                    {"since": since} if since else None, "campaign_open_details"
                )
                opens = fetch_paginated_collection(
                    worker_session,
                    base_url,
                    f"/reports/{campaign_id}/open-details",
                    "members",
                    params or {},
                    page_size,
                    max_attempts,
                    backoff_seconds,
                )
                for entry in opens:
                    # Derive last_open / total_opens / unique_opens from the
                    # nested `opens` array; Mailchimp does not populate them.
                    derived = _derive_open_details_fields(
                        entry, campaign, list_id, "campaign_open_details"
                    )
                    if not derived:
                        continue
                    row = derived[0]
                    if default_fields:
                        for key, value in default_fields.items():
                            row.setdefault(key, value)
                    rows.append(row)

            elif table == "campaign_click_details":
                wait_if_needed(MAILCHIMP_RATE_LIMITER)
                if click_urls_cache is None:
                    click_params = _with_fields(
                        {"since": since} if since else {}, "campaign_click_details"
                    )
                    click_urls_cache = fetch_paginated_collection(
                        worker_session,
                        base_url,
                        f"/reports/{campaign_id}/click-details",
                        "urls_clicked",
                        click_params,
                        page_size,
                        max_attempts,
                        backoff_seconds,
                    )
                for entry in click_urls_cache or []:
                    row = dict(entry)
                    row["campaign_id"] = campaign_id
                    row.setdefault("list_id", list_id)
                    if default_fields:
                        for key, value in default_fields.items():
                            row.setdefault(key, value)
                    rows.append(row)

            elif table == "campaign_click_members":
                if click_urls_cache is None:
                    wait_if_needed(MAILCHIMP_RATE_LIMITER)
                    click_params = _with_fields(
                        {"since": since} if since else {}, "campaign_click_details"
                    )
                    click_urls_cache = fetch_paginated_collection(
                        worker_session,
                        base_url,
                        f"/reports/{campaign_id}/click-details",
                        "urls_clicked",
                        click_params,
                        page_size,
                        max_attempts,
                        backoff_seconds,
                    )
                for url_data in click_urls_cache or []:
                    link_id = url_data.get("id")
                    if link_id:
                        wait_if_needed(MAILCHIMP_RATE_LIMITER)
                        params = _with_fields(
                            {"since": since} if since else None,
                            "campaign_click_members",
                        )
                        members = fetch_paginated_collection(
                            worker_session,
                            base_url,
                            f"/reports/{campaign_id}/click-details/{link_id}/members",
                            "members",
                            params or {},
                            page_size,
                            max_attempts,
                            backoff_seconds,
                        )
                        for entry in members:
                            # Derive click_count from clicks; last_click is
                            # filled by the post-process SQL that joins to
                            # campaign_email_activity.
                            derived = _derive_click_member_fields(
                                entry, campaign, list_id, "campaign_click_members"
                            )
                            if not derived:
                                continue
                            row = derived[0]
                            row["link_id"] = link_id
                            if default_fields:
                                for key, value in default_fields.items():
                                    row.setdefault(key, value)
                            rows.append(row)

            elif table == "campaign_domain_performance":
                wait_if_needed(MAILCHIMP_RATE_LIMITER)
                payload = mailchimp_request(
                    worker_session,
                    base_url,
                    "GET",
                    f"/reports/{campaign_id}/domain-performance",
                    None,
                    max_attempts,
                    backoff_seconds,
                )
                domains = payload.get("domains", [])
                for entry in domains:
                    row = dict(entry)
                    row["campaign_id"] = campaign_id
                    row.setdefault("list_id", list_id)
                    if default_fields:
                        for key, value in default_fields.items():
                            row.setdefault(key, value)
                    rows.append(row)

            elif table == "campaign_locations":
                wait_if_needed(MAILCHIMP_RATE_LIMITER)
                locations = fetch_paginated_collection(
                    worker_session,
                    base_url,
                    f"/reports/{campaign_id}/locations",
                    "locations",
                    {},
                    page_size,
                    max_attempts,
                    backoff_seconds,
                )
                for entry in locations:
                    row = dict(entry)
                    row["campaign_id"] = campaign_id
                    row.setdefault("list_id", list_id)
                    if default_fields:
                        for key, value in default_fields.items():
                            row.setdefault(key, value)
                    rows.append(row)

            # Write rows to Parquet instead of JSONL
            os.makedirs(temp_dir, exist_ok=True)
            if rows:
                # Add execution metadata (extracted_at, execution_id, source)
                if execution_id:
                    set_execution_metadata(rows, execution_id, source="mailchimp")

                from shared.parquet_writer import ParquetWriter

                file_path = os.path.join(temp_dir, f"{table}_{campaign_id}.parquet")
                writer = ParquetWriter(file_path, buffer_size=10000)
                writer.write_batch(rows)
                writer.close()
                row_counts[table] = len(rows)

        except Exception as exc:
            logger.warning(
                f"Failed to extract {table} for campaign {campaign_id}: {exc}"
            )

    return row_counts


def rest_extract_campaign_tables(
    session: requests.Session,
    base_url: str,
    campaigns: List[Dict[str, Any]],
    table_configs: Dict[str, Dict[str, Any]],
    requested_tables: List[str],
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    default_fields: Dict[str, Any],
    tenant_mapping: Dict[str, Any],
    lists_cache: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    temp_dir: str,
    parallel_workers: int = 3,
    execution_id: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Extract campaign tables using REST API with parallel processing.
    More reliable than batch API for large date ranges.

    Returns: Dict[table_name, {'files': [parquet_paths], 'row_count': int}]
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from shared.parquet_writer import ParquetWriter

    logger.info(
        f"REST API extraction: processing {len(campaigns)} campaigns across {len(requested_tables)} tables"
    )
    logger.info(f"Using {parallel_workers} parallel workers")

    # The REST workers get no table configs, so seed their `fields`
    # projections here while the configs are in scope.
    prime_rest_fields_cache(table_configs)

    # Compute since timestamp once for all campaigns (uses first table's config for lookback override)
    first_table_config = next(iter(table_configs.values()), {}) if table_configs else {}
    since = compute_since_timestamp(
        refresh_mode, lookback_days, start_date, first_table_config
    )
    if since:
        logger.info(f"Incremental mode: fetching records changed since {since}")
    else:
        logger.info("Full refresh: no since filter")

    results = {}
    for table in requested_tables:
        parquet_file = os.path.join(temp_dir, f"{table}.parquet")
        results[table] = {
            "files": [parquet_file],
            "row_count": 0,
            "writer": ParquetWriter(parquet_file, buffer_size=10000),
        }

    try:
        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            futures = []
            for campaign in campaigns:
                future = executor.submit(
                    _rest_extract_campaign_all_tables,
                    campaign,
                    requested_tables,
                    session,
                    base_url,
                    table_configs,
                    default_fields,
                    tenant_mapping,
                    lists_cache,
                    page_size,
                    max_attempts,
                    backoff_seconds,
                    since,
                )
                futures.append((future, campaign["id"]))

            completed_count = 0
            for future, campaign_id in futures:
                try:
                    campaign_rows = future.result()
                    completed_count += 1

                    # Write rows to Parquet files
                    for table, rows in campaign_rows.items():
                        if rows and table in results:
                            # Add execution metadata (extracted_at, execution_id, source)
                            if execution_id:
                                set_execution_metadata(
                                    rows, execution_id, source="mailchimp"
                                )
                            writer = results[table]["writer"]
                            writer.write_batch(rows)
                            results[table]["row_count"] += len(rows)

                    if completed_count % 50 == 0:
                        logger.info(
                            f"[{completed_count}/{len(campaigns)}] Campaigns processed"
                        )

                except Exception as exc:
                    logger.warning(f"Failed to process campaign {campaign_id}: {exc}")

            logger.info(f"[{completed_count}/{len(campaigns)}] All campaigns processed")

    finally:
        # Close all Parquet writers
        for table_info in results.values():
            if "writer" in table_info:
                table_info["writer"].close()
                del table_info["writer"]

    return results


def _rest_extract_campaign_all_tables(
    campaign: Dict[str, Any],
    requested_tables: List[str],
    session: requests.Session,
    base_url: str,
    table_configs: Dict[str, Dict[str, Any]],
    default_fields: Dict[str, Any],
    tenant_mapping: Dict[str, Any],
    lists_cache: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    since: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Extract all requested tables for a single campaign using REST API."""
    campaign_id = campaign.get("id")
    list_id = campaign.get("recipients", {}).get("list_id")

    results = {}

    since_dt = parse_mailchimp_timestamp(since) if since else None

    for table in requested_tables:
        try:
            if table == "campaign_sent_to":
                rows = _rest_extract_campaign_sent_to(
                    campaign_id,
                    session,
                    base_url,
                    page_size,
                    max_attempts,
                    backoff_seconds,
                    since=since,
                )
            elif table == "campaign_email_activity":
                # Mailchimp's /email-activity returns one record per recipient with a
                # nested 'activity' array. Explode it into one row per event so
                # activity_timestamp / action / url / device land as top-level columns.
                # Mirrors the Batch-path flattener already used elsewhere.
                emails_records = _rest_extract_campaign_email_activity(
                    campaign_id,
                    session,
                    base_url,
                    page_size,
                    max_attempts,
                    backoff_seconds,
                    since=since,
                )
                rows = []
                for item in emails_records:
                    rows.extend(
                        _explode_email_activity_actions(
                            item, campaign, list_id, "campaign_email_activity", since_dt
                        )
                    )
            elif table == "campaign_open_details":
                # Mailchimp's /open-details returns the events under nested
                # `opens` array but never populates last_open / total_opens /
                # unique_opens at the top level. Derive them from the array so
                # reporting can filter and aggregate on these columns.
                members = _rest_extract_campaign_open_details(
                    campaign_id,
                    session,
                    base_url,
                    page_size,
                    max_attempts,
                    backoff_seconds,
                    since=since,
                )
                rows = []
                for item in members:
                    rows.extend(
                        _derive_open_details_fields(
                            item, campaign, list_id, "campaign_open_details"
                        )
                    )
            elif table == "campaign_click_details":
                rows = _rest_extract_campaign_click_details(
                    campaign_id,
                    session,
                    base_url,
                    page_size,
                    max_attempts,
                    backoff_seconds,
                    since=since,
                )
            elif table == "campaign_click_members":
                # Mailchimp's /click-details/{link_id}/members returns a
                # per-member click count under `clicks` but never populates
                # `click_count`. Derive click_count from clicks; last_click
                # comes from the post-process SQL that joins to email_activity.
                members = _rest_extract_campaign_click_members(
                    campaign_id,
                    session,
                    base_url,
                    page_size,
                    max_attempts,
                    backoff_seconds,
                    since=since,
                )
                rows = []
                for item in members:
                    rows.extend(
                        _derive_click_member_fields(
                            item, campaign, list_id, "campaign_click_members"
                        )
                    )
            elif table == "campaign_domain_performance":
                rows = _rest_extract_campaign_domain_performance(
                    campaign_id, session, base_url, max_attempts, backoff_seconds
                )
            elif table == "campaign_locations":
                rows = _rest_extract_campaign_locations(
                    campaign_id, session, base_url, max_attempts, backoff_seconds
                )
            else:
                rows = []

            # Enrich with campaign_id and tenant info
            enriched_rows = []
            for row in rows:
                row["campaign_id"] = campaign_id
                if list_id:
                    row["list_id"] = list_id
                # For tables where subscriber_hash is part of PK, derive from email if missing
                if table in (
                    "campaign_open_details",
                    "campaign_click_members",
                    "campaign_sent_to",
                ):
                    if not row.get("subscriber_hash") and row.get("email_address"):
                        row["subscriber_hash"] = derive_email_id(
                            campaign_id, row["email_address"]
                        )
                    if not row.get("subscriber_hash"):
                        continue  # Skip rows that can't be uniquely identified
                enrich_record_with_tenant_info(row, tenant_mapping, lists_cache)
                enriched_rows.append(row)

            results[table] = enriched_rows

        except Exception as exc:
            logger.debug(f"Failed to extract {table} for campaign {campaign_id}: {exc}")
            results[table] = []

    return results


def _rest_extract_campaign_sent_to(
    campaign_id: str,
    session: requests.Session,
    base_url: str,
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    since: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Extract sent-to using REST API."""
    params = {"since": since} if since else {}
    return fetch_paginated_collection(
        session,
        base_url,
        f"/reports/{campaign_id}/sent-to",
        "sent_to",
        params,
        page_size,
        max_attempts,
        backoff_seconds,
    )


def _rest_extract_campaign_email_activity(
    campaign_id: str,
    session: requests.Session,
    base_url: str,
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    since: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Extract email activity using REST API."""
    params = {"since": since} if since else {}
    return fetch_paginated_collection(
        session,
        base_url,
        f"/reports/{campaign_id}/email-activity",
        "emails",
        params,
        page_size,
        max_attempts,
        backoff_seconds,
    )


def _rest_extract_campaign_open_details(
    campaign_id: str,
    session: requests.Session,
    base_url: str,
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    since: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Extract open details using REST API."""
    params = {"since": since} if since else {}
    return fetch_paginated_collection(
        session,
        base_url,
        f"/reports/{campaign_id}/open-details",
        "members",
        params,
        page_size,
        max_attempts,
        backoff_seconds,
    )


def _rest_extract_campaign_click_details(
    campaign_id: str,
    session: requests.Session,
    base_url: str,
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    since: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Extract click details (URL-level aggregates) using REST API."""
    params = {"since": since} if since else {}
    return fetch_paginated_collection(
        session,
        base_url,
        f"/reports/{campaign_id}/click-details",
        "urls_clicked",
        params,
        page_size,
        max_attempts,
        backoff_seconds,
    )


def _rest_extract_campaign_click_members(
    campaign_id: str,
    session: requests.Session,
    base_url: str,
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    since: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Extract click members using REST API.

    Two-step fetch: first get the list of clicked URLs, then for each URL get the members.
    The endpoint /reports/{campaign_id}/click-details/members does NOT exist — each link_id
    must be fetched individually via /reports/{campaign_id}/click-details/{link_id}/members.
    """
    params = {"since": since} if since else {}
    urls_clicked = fetch_paginated_collection(
        session,
        base_url,
        f"/reports/{campaign_id}/click-details",
        "urls_clicked",
        params,
        page_size,
        max_attempts,
        backoff_seconds,
    )
    all_members = []
    for link_entry in urls_clicked or []:
        link_id = link_entry.get("id") or link_entry.get("link_id")
        if not link_id:
            continue
        try:
            members = fetch_paginated_collection(
                session,
                base_url,
                f"/reports/{campaign_id}/click-details/{link_id}/members",
                "members",
                params,
                page_size,
                max_attempts,
                backoff_seconds,
            )
            for member in members:
                row = dict(member)
                row["link_id"] = link_id
                all_members.append(row)
        except Exception as exc:
            logger.debug(
                "Failed to fetch members for campaign %s link %s: %s",
                campaign_id,
                link_id,
                exc,
            )
    return all_members


def _rest_extract_campaign_domain_performance(
    campaign_id: str,
    session: requests.Session,
    base_url: str,
    max_attempts: int,
    backoff_seconds: float,
) -> List[Dict[str, Any]]:
    """Extract domain performance using REST API."""
    payload = mailchimp_request(
        session,
        base_url,
        "GET",
        f"/reports/{campaign_id}/domain-performance",
        None,
        max_attempts,
        backoff_seconds,
    )
    return payload.get("domains", [])


def _rest_extract_campaign_locations(
    campaign_id: str,
    session: requests.Session,
    base_url: str,
    max_attempts: int,
    backoff_seconds: float,
) -> List[Dict[str, Any]]:
    """Extract locations using REST API."""
    payload = mailchimp_request(
        session,
        base_url,
        "GET",
        f"/reports/{campaign_id}/locations",
        None,
        max_attempts,
        backoff_seconds,
    )
    return payload.get("locations", [])


def extract_campaign_multi_table(
    session: requests.Session,
    base_url: str,
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    requested_tables: List[str],
    table_configs: Dict[str, Dict[str, Any]],
    cache: Dict[str, Any],
    default_fields: Dict[str, Any],
    tenant_mapping: Dict[str, Any],
    lists_cache: Dict[str, Any],
    page_size: int,
    max_attempts: int,
    backoff_seconds: float,
    temp_dir: str = "/tmp/mailchimp",
    parallel_workers: int = 3,
    use_batch_api: bool = False,
    execution_id: Optional[str] = None,
    batch_table_workers: int = 1,
    table_strategies: Optional[Dict[str, str]] = None,
) -> Dict[str, List[str]]:
    # Extract multiple campaign-based tables in a single pass
    # Returns dict of table_name -> list of local Parquet file paths
    tenant_mapping = tenant_mapping or {}
    lists_cache = lists_cache or {}
    # Initialize rate limiter
    extractor_cfg = cache.get("extractor", {})
    rate_limit_cfg = extractor_cfg.get("rate_limit", {})
    requests_per_second = rate_limit_cfg.get("requests_per_second", 9.0)
    configure_mailchimp_rate_limit(requests_per_second)

    parallel_workers = max(1, min(parallel_workers, MAILCHIMP_MAX_CONCURRENT_REQUESTS))

    prime_rest_fields_cache(table_configs)

    logger.info(f"Multi-table extraction: {len(requested_tables)} tables")
    logger.info(f"Tables: {', '.join(requested_tables)}")

    # Fetch campaigns once
    since = compute_since_timestamp(
        refresh_mode, lookback_days, start_date, table_configs[requested_tables[0]]
    )
    filtered_campaigns, _ = get_filtered_sent_campaigns(
        session,
        base_url,
        cache,
        "multi_table",
        since,
        page_size,
        max_attempts,
        backoff_seconds,
        "rest",
    )

    total_campaigns = len(filtered_campaigns)
    logger.info(
        f"Processing {total_campaigns} campaigns across {len(requested_tables)} tables"
    )

    if total_campaigns == 0:
        return {table: [] for table in requested_tables}

    campaign_table_requirements: Dict[str, Set[str]] = {}
    selected_campaigns: List[Dict[str, Any]] = []
    skipped_campaigns = 0

    for campaign in filtered_campaigns:
        campaign_id = campaign.get("id")
        if not campaign_id:
            continue
        applicable_tables = {
            table
            for table in requested_tables
            if should_process_campaign_for_table(campaign, table)
        }
        if applicable_tables:
            campaign_table_requirements[campaign_id] = applicable_tables
            selected_campaigns.append(campaign)
        else:
            skipped_campaigns += 1

    if skipped_campaigns:
        logger.info(
            "Campaign activity filter removed %d campaigns with no relevant activity",
            skipped_campaigns,
        )

    if not selected_campaigns:
        logger.info("No campaigns matched requested tables after activity filtering")
        return {table: [] for table in requested_tables}

    filtered_campaigns = selected_campaigns
    total_campaigns = len(filtered_campaigns)
    logger.info(f"Campaigns remaining after filtering: {total_campaigns}")

    # Create temp directory
    os.makedirs(temp_dir, exist_ok=True)

    # Process campaigns in parallel
    completed = 0
    total_rows_by_table = {table: 0 for table in requested_tables}
    handled_tables: Set[str] = set()

    batch_tables, rest_tables = _partition_campaign_tables_by_strategy(
        requested_tables,
        table_strategies,
        use_batch_api,
    )

    batch_results: Dict[str, Dict[str, Any]] = {}
    if batch_tables:
        logger.info(
            "Using Mailchimp Batch API for campaign tables: %s",
            ", ".join(batch_tables),
        )
        batch_results.update(
            batch_extract_campaign_tables(
                session,
                base_url,
                filtered_campaigns,
                table_configs,
                batch_tables,
                refresh_mode,
                lookback_days,
                start_date,
                default_fields,
                tenant_mapping,
                lists_cache,
                page_size,
                max_attempts,
                backoff_seconds,
                temp_dir,
                execution_id=execution_id,
                max_workers=batch_table_workers,
            )
        )

    if rest_tables:
        logger.info(
            "Using REST API for campaign tables: %s",
            ", ".join(rest_tables),
        )
        batch_results.update(
            rest_extract_campaign_tables(
                session,
                base_url,
                filtered_campaigns,
                table_configs,
                rest_tables,
                refresh_mode,
                lookback_days,
                start_date,
                default_fields,
                tenant_mapping,
                lists_cache,
                page_size,
                max_attempts,
                backoff_seconds,
                temp_dir,
                parallel_workers,
                execution_id=execution_id,
            )
        )

    # Store files returned by the selected extraction strategies.
    batch_files = {table: [] for table in requested_tables}

    for table_name, info in batch_results.items():
        handled_tables.add(table_name)
        row_count = info.get("row_count", 0) or 0
        files = info.get("files", []) or []
        total_rows_by_table[table_name] = row_count
        batch_files[table_name] = files
        logger.info(
            "%s: Extracted %s rows via selected strategy", table_name, f"{row_count:,}"
        )

    if handled_tables:
        for campaign_id in list(campaign_table_requirements.keys()):
            remaining = campaign_table_requirements[campaign_id] - handled_tables
            if remaining:
                campaign_table_requirements[campaign_id] = remaining
            else:
                del campaign_table_requirements[campaign_id]

    campaigns_for_workers = [
        c for c in filtered_campaigns if campaign_table_requirements.get(c.get("id"))
    ]
    campaigns_handled_by_batch = len(filtered_campaigns) - len(campaigns_for_workers)
    if campaigns_handled_by_batch:
        logger.info(
            "Selected extraction strategies satisfied %d campaigns with no worker fallback",
            campaigns_handled_by_batch,
        )

    filtered_campaigns = campaigns_for_workers
    total_campaigns = len(filtered_campaigns)
    if handled_tables:
        logger.info("Campaigns remaining after batch phase: %d", total_campaigns)

    remaining_tables = [
        table for table in requested_tables if table not in handled_tables
    ]

    lock = threading.Lock()

    if not remaining_tables or total_campaigns == 0:
        logger.info(
            "Selected extraction strategies covered all required campaign tables"
        )
    elif parallel_workers > 1:
        logger.info(
            f"Using {parallel_workers} parallel workers with rate limiting at {requests_per_second} req/sec"
        )

        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            future_to_campaign = {
                executor.submit(
                    _process_campaign_all_tables,
                    campaign,
                    remaining_tables,
                    None,
                    base_url,
                    since,
                    page_size,
                    max_attempts,
                    backoff_seconds,
                    default_fields,
                    temp_dir,
                    campaign_table_requirements.get(campaign.get("id")),
                    cache.get("api_key"),
                    cache.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS),
                    execution_id,
                ): campaign
                for campaign in filtered_campaigns
            }

            for future in as_completed(future_to_campaign):
                campaign = future_to_campaign[future]
                try:
                    row_counts = future.result()
                    with lock:
                        completed += 1
                        for table, count in row_counts.items():
                            total_rows_by_table[table] += count

                        # Log progress every 50 campaigns
                        if completed % 50 == 0 or completed == total_campaigns:
                            progress_pct = (completed / total_campaigns) * 100
                            total_rows = sum(total_rows_by_table.values())
                            logger.info(
                                f"Progress: {completed}/{total_campaigns} campaigns ({progress_pct:.1f}%), {total_rows:,} total rows"
                            )

                except Exception as exc:
                    logger.error(
                        f"Error processing campaign {campaign.get('id')}: {exc}"
                    )
    else:
        # Serial processing
        for idx, campaign in enumerate(filtered_campaigns, start=1):
            row_counts = _process_campaign_all_tables(
                campaign,
                remaining_tables,
                session,
                base_url,
                since,
                page_size,
                max_attempts,
                backoff_seconds,
                default_fields,
                temp_dir,
                campaign_table_requirements.get(campaign.get("id")),
                execution_id=execution_id,
            )
            completed += 1
            for table, count in row_counts.items():
                total_rows_by_table[table] += count

            if idx % 50 == 0 or idx == total_campaigns:
                progress_pct = (idx / total_campaigns) * 100
                total_rows = sum(total_rows_by_table.values())
                logger.info(
                    f"Progress: {idx}/{total_campaigns} campaigns ({progress_pct:.1f}%), {total_rows:,} total rows"
                )

    # Gather all Parquet files per table (combine batch and worker files)
    result_files = {table: [] for table in requested_tables}
    for table in requested_tables:
        # Start with batch-extracted files
        files = list(batch_files.get(table, []))

        # Add worker-extracted files
        pattern = os.path.join(temp_dir, f"{table}_*.parquet")
        worker_files = glob.glob(pattern)

        # Combine, avoiding duplicates
        for wf in worker_files:
            if wf not in files:
                files.append(wf)

        result_files[table] = sorted(files)
        logger.info(
            f"{table}: {total_rows_by_table[table]:,} rows in {len(files)} files"
        )

    total_campaigns_processed = completed + campaigns_handled_by_batch
    logger.info(
        "Multi-table extraction complete: %d campaigns (%d selected-strategy, %d workers), %s total rows",
        total_campaigns_processed,
        campaigns_handled_by_batch,
        completed,
        f"{sum(total_rows_by_table.values()):,}",
    )

    return result_files
