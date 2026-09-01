"""Shared helpers for account-aware extractors."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Iterable, Optional


def set_execution_metadata(
    records: Iterable[Dict[str, object]],
    execution_id: str,
    *,
    source: Optional[str] = None,
) -> None:
    """Annotate each record with execution-scoped metadata."""

    for record in records or []:
        if record is None:
            continue
        record["extracted_at"] = datetime.now(timezone.utc)
        record["execution_id"] = execution_id
        if source is not None:
            record["source"] = source


def enrich_record_with_tenant_info(
    record: Dict[str, object],
    tenant_mapping: Optional[Dict[str, str]],
    lists_cache: Optional[Dict[str, Dict[str, object]]] = None,
) -> None:
    """Inject tenant metadata based on Mailchimp list identifiers."""

    if record is None:
        return

    list_id = record.get("list_id")  # type: ignore[arg-type]

    if not list_id:
        record["tenant_id"] = None
        record["tenant_name"] = None
        return

    record["tenant_id"] = list_id

    tenant_name = tenant_mapping.get(list_id) if tenant_mapping else None
    if not tenant_name and lists_cache and list_id in lists_cache:
        tenant_name = lists_cache[list_id].get("name")  # type: ignore[arg-type]

    record["tenant_name"] = tenant_name or list_id


__all__ = [
    "set_execution_metadata",
    "enrich_record_with_tenant_info",
]
