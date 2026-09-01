"""Mailchimp metadata caching utilities."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from shared.timestamp_utils import parse_mailchimp_timestamp

_UTC = timezone.utc


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value != value:  # NaN check
            return None
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        stripped = stripped.replace(",", "")
        try:
            return int(stripped)
        except ValueError:
            try:
                return int(float(stripped))
            except ValueError:
                return None
    return None


def _serialize_for_json(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _to_utc_naive(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=_UTC)
    else:
        value = value.astimezone(_UTC)
    return value.replace(tzinfo=None)


def _serialize_timestamp(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=_UTC)
    return value.astimezone(_UTC).isoformat()


def _parse_date_str(value: Optional[str], end_of_day: bool = False) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.strptime(value, "%Y-%m-%d")
        if end_of_day:
            dt = dt.replace(hour=23, minute=59, second=59)
        return dt.replace(tzinfo=_UTC)
    except ValueError:
        try:
            parsed = parse_mailchimp_timestamp(value)
            if parsed and end_of_day:
                parsed = parsed.replace(hour=23, minute=59, second=59)
            return parsed
        except Exception:
            return None


def merge_records_by_key(existing: List[Dict[str, Any]], new_records: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for record in existing or []:
        record_id = record.get(key)
        if record_id:
            index[record_id] = record
    for record in new_records or []:
        record_id = record.get(key)
        if record_id:
            index[record_id] = record
    return list(index.values())


def filter_preloaded_campaigns(
    campaigns: List[Dict[str, Any]],
    since: Optional[str],
    extra_params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    filtered = campaigns or []
    if since:
        since_dt = parse_mailchimp_timestamp(since)
        if since_dt:
            filtered = [
                campaign
                for campaign in filtered
                if not campaign.get("send_time")
                or (
                    parse_mailchimp_timestamp(campaign.get("send_time"))
                    or datetime.min.replace(tzinfo=_UTC)
                )
                >= since_dt
            ]

    if extra_params and extra_params.get("status"):
        statuses = {
            status.strip().lower()
            for status in str(extra_params["status"]).split(",")
            if status.strip()
        }
        if statuses:
            filtered = [
                campaign
                for campaign in filtered
                if (campaign.get("status") or "").lower() in statuses
            ]

    return filtered


class MailchimpMetadataManager:
    """Persist and retrieve Mailchimp metadata snapshots in BigQuery."""

    def __init__(self, bq_client: bigquery.Client, project: str, dataset: str):
        self.client = bq_client
        self.project = project
        self.dataset = dataset
        self._campaign_table_id = f"{self.project}.{self.dataset}.metadata_campaigns"
        self._list_table_id = f"{self.project}.{self.dataset}.metadata_lists"
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        campaign_schema = [
            bigquery.SchemaField("account_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("list_id", "STRING"),
            bigquery.SchemaField("send_time", "TIMESTAMP"),
            bigquery.SchemaField("create_time", "TIMESTAMP"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("emails_sent", "INT64"),
            bigquery.SchemaField("payload", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("last_refreshed", "TIMESTAMP", mode="REQUIRED"),
        ]

        list_schema = [
            bigquery.SchemaField("account_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("list_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("date_created", "TIMESTAMP"),
            bigquery.SchemaField("member_count", "INT64"),
            bigquery.SchemaField("payload", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("last_refreshed", "TIMESTAMP", mode="REQUIRED"),
        ]

        self._ensure_table(self._campaign_table_id, campaign_schema, partition_field="last_refreshed")
        self._ensure_table(self._list_table_id, list_schema, partition_field="last_refreshed")

    def _ensure_table(
        self,
        table_id: str,
        schema: List[bigquery.SchemaField],
        partition_field: Optional[str] = None,
    ) -> None:
        try:
            self.client.get_table(table_id)
        except NotFound:
            table = bigquery.Table(table_id, schema=schema)
            if partition_field:
                table.time_partitioning = bigquery.TimePartitioning(field=partition_field)
            self.client.create_table(table)

    def load_campaigns(
        self,
        account_name: str,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        params = [bigquery.ScalarQueryParameter("account_name", "STRING", account_name)]
        conditions = ["account_name = @account_name"]
        if start_dt:
            params.append(bigquery.ScalarQueryParameter("start_dt", "TIMESTAMP", _to_utc_naive(start_dt)))
            conditions.append("(send_time IS NULL OR send_time >= @start_dt)")
        if end_dt:
            params.append(bigquery.ScalarQueryParameter("end_dt", "TIMESTAMP", _to_utc_naive(end_dt)))
            conditions.append("(send_time IS NULL OR send_time <= @end_dt)")
        query = f"SELECT payload FROM `{self._campaign_table_id}` WHERE {' AND '.join(conditions)}"
        job = self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
        campaigns: List[Dict[str, Any]] = []
        for row in job:
            payload = row.get("payload")
            if payload:
                try:
                    campaigns.append(json.loads(payload))
                except json.JSONDecodeError:
                    continue
        return campaigns

    def load_lists(self, account_name: str) -> List[Dict[str, Any]]:
        query = f"SELECT payload FROM `{self._list_table_id}` WHERE account_name = @account_name"
        job = self.client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("account_name", "STRING", account_name)]
            ),
        )
        lists: List[Dict[str, Any]] = []
        for row in job:
            payload = row.get("payload")
            if payload:
                try:
                    lists.append(json.loads(payload))
                except json.JSONDecodeError:
                    continue
        return lists

    def upsert_campaigns(self, account_name: str, campaigns: List[Dict[str, Any]]) -> None:
        if not campaigns:
            return
        rows = []
        last_refreshed = datetime.now(tz=_UTC).isoformat()
        for campaign in campaigns:
            campaign_id = campaign.get("id")
            if not campaign_id:
                continue
            recipients = campaign.get("recipients") or {}
            send_time = _serialize_timestamp(parse_mailchimp_timestamp(campaign.get("send_time")))
            create_time = _serialize_timestamp(parse_mailchimp_timestamp(campaign.get("create_time")))
            row = {
                "account_name": account_name,
                "campaign_id": campaign_id,
                "list_id": recipients.get("list_id"),
                "send_time": send_time,
                "create_time": create_time,
                "status": campaign.get("status"),
                "emails_sent": _coerce_int(campaign.get("emails_sent")),
                "payload": json.dumps(campaign, default=_serialize_for_json),
                "last_refreshed": last_refreshed,
            }
            rows.append(row)
        if not rows:
            return
        temp_table_id = f"{self.project}.{self.dataset}._tmp_campaigns_{uuid.uuid4().hex[:8]}"
        load_job = self.client.load_table_from_json(
            rows,
            temp_table_id,
            job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE),
        )
        load_job.result()
        merge_sql = f"""
            MERGE `{self._campaign_table_id}` T
            USING `{temp_table_id}` S
            ON T.account_name = S.account_name AND T.campaign_id = S.campaign_id
            WHEN MATCHED THEN UPDATE SET
              list_id = S.list_id,
              send_time = S.send_time,
              create_time = S.create_time,
              status = S.status,
              emails_sent = S.emails_sent,
              payload = S.payload,
              last_refreshed = S.last_refreshed
            WHEN NOT MATCHED THEN INSERT (account_name, campaign_id, list_id, send_time, create_time, status, emails_sent, payload, last_refreshed)
            VALUES (S.account_name, S.campaign_id, S.list_id, S.send_time, S.create_time, S.status, S.emails_sent, S.payload, S.last_refreshed)
        """
        self.client.query(merge_sql).result()
        self.client.delete_table(temp_table_id, not_found_ok=True)

    def upsert_lists(self, account_name: str, lists: List[Dict[str, Any]]) -> None:
        if not lists:
            return
        rows = []
        last_refreshed = datetime.now(tz=_UTC).isoformat()
        for lst in lists:
            list_id = lst.get("id")
            if not list_id:
                continue
            date_created = _serialize_timestamp(parse_mailchimp_timestamp(lst.get("date_created")))
            row = {
                "account_name": account_name,
                "list_id": list_id,
                "name": lst.get("name"),
                "date_created": date_created,
                "member_count": _coerce_int((lst.get("stats") or {}).get("member_count") or lst.get("member_count")),
                "payload": json.dumps(lst, default=_serialize_for_json),
                "last_refreshed": last_refreshed,
            }
            rows.append(row)
        if not rows:
            return
        temp_table_id = f"{self.project}.{self.dataset}._tmp_lists_{uuid.uuid4().hex[:8]}"
        load_job = self.client.load_table_from_json(
            rows,
            temp_table_id,
            job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE),
        )
        load_job.result()
        merge_sql = f"""
            MERGE `{self._list_table_id}` T
            USING `{temp_table_id}` S
            ON T.account_name = S.account_name AND T.list_id = S.list_id
            WHEN MATCHED THEN UPDATE SET
              name = S.name,
              date_created = S.date_created,
              member_count = S.member_count,
              payload = S.payload,
              last_refreshed = S.last_refreshed
            WHEN NOT MATCHED THEN INSERT (account_name, list_id, name, date_created, member_count, payload, last_refreshed)
            VALUES (S.account_name, S.list_id, S.name, S.date_created, S.member_count, S.payload, S.last_refreshed)
        """
        self.client.query(merge_sql).result()
        self.client.delete_table(temp_table_id, not_found_ok=True)


def resolve_run_window(
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    end_date: Optional[str],
) -> Tuple[Optional[datetime], Optional[datetime]]:
    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None
    if start_date:
        start_dt = _parse_date_str(start_date)
    elif refresh_mode != "full" and lookback_days:
        start_dt = datetime.now(tz=_UTC) - timedelta(days=lookback_days)

    if end_date:
        end_dt = _parse_date_str(end_date, end_of_day=True)

    return start_dt, end_dt


__all__ = [
    "MailchimpMetadataManager",
    "filter_preloaded_campaigns",
    "merge_records_by_key",
    "resolve_run_window",
]
