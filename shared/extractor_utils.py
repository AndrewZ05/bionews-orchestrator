"""Shared helper utilities for data extractors."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional


def sanitize_bq_column_name(name: str) -> str:
    """
    Make a string a stable, valid BigQuery column name.

    Maps every run of non [A-Za-z0-9_] chars to a single underscore, collapses
    repeats, strips edge underscores, prefixes a leading digit, lowercases. The point
    is STABILITY: the same source name must always produce the SAME column, so a name
    with spaces/dots/dashes (e.g. Meta's "offsite_conversion.fb_pixel_custom.MS - Ad
    impression") does not get partially cleaned, land in BigQuery differently, and
    then be re-added as a NEW column with a _1 suffix on every run.
    """
    if not name:
        return "unknown_column"
    col = re.sub(r"[^A-Za-z0-9_]", "_", name)
    col = re.sub(r"_+", "_", col).strip("_")
    if col and col[0].isdigit():
        col = "_" + col
    return (col[:300].lower()) if col else "unknown_column"


def apply_table_affix(
    table_name: str,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
    *,
    prefix_separator: str = "",
    suffix_separator: str = "",
) -> str:
    """Apply optional prefix/suffix to a table name.

    Args:
        table_name: Base table identifier.
        prefix: Optional prefix to prepend.
        suffix: Optional suffix to append.
        prefix_separator: Separator inserted between prefix and table when prefix provided.
        suffix_separator: Separator inserted between table and suffix when suffix provided.

    Returns:
        The formatted table name with affixes applied.
    """

    if not table_name:
        raise ValueError("table_name is required")

    result = table_name
    if prefix:
        if prefix_separator and not prefix.endswith(prefix_separator):
            result = f"{prefix}{prefix_separator}{result}"
        else:
            result = f"{prefix}{result}"

    if suffix:
        if suffix_separator and not suffix.startswith(suffix_separator):
            result = f"{result}{suffix_separator}{suffix}"
        else:
            result = f"{result}{suffix}"

    return result


def get_available_tables(
    config: Dict[str, Any],
    group: Optional[str] = None,
    *,
    include_inactive: bool = False,
) -> List[str]:
    """Return tables enabled for extraction from configuration."""

    resources = config.get("resources", {})
    tables: List[str] = []
    for name, resource_cfg in resources.items():
        if group and resource_cfg.get("group") != group:
            continue
        active_value = resource_cfg.get("active", True)
        if not include_inactive:
            # Skip if explicitly False (boolean)
            if active_value is False or active_value == False:
                continue
            # Skip if string value is "false" or "child" (case-insensitive)
            if isinstance(active_value, str):
                active_lower = active_value.lower().strip()
                if active_lower in ("false", "child", "no", "n", "0"):
                    continue
        tables.append(name)
    return sorted(tables)


def coerce_list(values: Optional[Iterable[Any]]) -> List[Any]:
    """Ensure values are represented as a list."""

    if values is None:
        return []
    if isinstance(values, list):
        return values
    if isinstance(values, tuple):
        return list(values)
    return [values]


__all__ = [
    "apply_table_affix",
    "get_available_tables",
    "coerce_list",
]
