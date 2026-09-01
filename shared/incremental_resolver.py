"""
Centralized incremental date/ID resolution for all extractors.

Consolidates the duplicated logic from wordpress_extractor.py (lines 1005-1048)
and limesurvey_extractor.py (lines 1073-1082, 1444-1455) into a single
function that handles all incremental strategies.

Expects the new canonical YAML format where incremental config lives in a
nested ``incremental:`` block within each resource definition.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


def get_incremental_config(table_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract the incremental config block from a table resource config.

    Reads from the canonical ``incremental:`` nested block.

    Returns:
        Dict with keys: strategy, fields, lookback_days, initial_value.
        Defaults to ``{'strategy': 'none'}`` if not configured.
    """
    inc = table_config.get("incremental", {})
    if not inc:
        return {
            "strategy": "none",
            "fields": [],
            "lookback_days": 7,
            "initial_value": None,
        }

    return {
        "strategy": inc.get("strategy", "date"),
        "fields": inc.get("fields", []),
        "lookback_days": inc.get("lookback_days", 7),
        "initial_value": inc.get("initial_value"),
    }


def resolve_incremental_dates(
    table_config: Dict[str, Any],
    refresh_mode: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lookback_days: Optional[int] = None,
    watermark_start: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Resolve effective start/end dates for a table extraction.

    Logic (replaces duplicated code from wordpress/limesurvey extractors):

    1. ``refresh_mode == 'full'``: returns (None, None) – extract everything.
    2. Explicit ``start_date``/``end_date`` provided: use those directly.
    3. Strategy ``'date'``: calculate from lookback_days or initial_value.
    4. Strategy ``'id'``: returns (None, end_date) – no date filtering.
    5. Strategy ``'via_parent'``: returns (None, None) – parent handles it.
    6. Strategy ``'none'``: returns (None, None).

    Args:
        table_config: Resource config dict from YAML.
        refresh_mode: ``'full'`` or ``'incremental'``.
        start_date: Explicit start date override (ISO string).
        end_date: Explicit end date override (ISO string).
        lookback_days: Override for lookback_days from config.

    Returns:
        Tuple of (effective_start_date, effective_end_date) as ISO strings
        or (None, None) if no date filtering should be applied.
    """
    # Full refresh = no date filtering
    if refresh_mode == "full":
        return None, None

    # Explicit dates override everything
    if start_date and end_date:
        return start_date, end_date

    inc = get_incremental_config(table_config)
    strategy = inc["strategy"]

    if strategy == "date":
        effective_lookback = lookback_days or inc["lookback_days"] or 7

        if start_date:
            # Explicit operator override always wins.
            effective_start = start_date
        elif watermark_start:
            # Adaptive watermark (opt-in via incremental.use_watermark): start from
            # the caller-resolved watermark window instead of a blanket lookback.
            # The caller (orchestrator) computes this via shared.watermark only when
            # the table opts in, so the default path below is unchanged.
            effective_start = watermark_start
        else:
            # Default: fixed lookback window.
            effective_start = (
                datetime.utcnow() - timedelta(days=effective_lookback)
            ).strftime("%Y-%m-%d")

        # Fall back to initial_value if no other start
        if not effective_start and inc["initial_value"]:
            effective_start = inc["initial_value"]

        effective_end = end_date  # may be None (open-ended)
        return effective_start, effective_end

    if strategy == "id":
        # ID-based incremental – no date filtering, just end_date as upper bound
        return None, end_date

    if strategy in ("via_parent", "none"):
        return None, None

    # Unknown strategy – treat as no filtering
    logger.warning(
        f"Unknown incremental strategy '{strategy}', treating as full refresh"
    )
    return None, None


def get_primary_incremental_field(table_config: Dict[str, Any]) -> Optional[str]:
    """
    Return the primary incremental field name (first in the ``fields`` list).

    Used by extractors that need the field name for WHERE clauses.
    """
    inc = get_incremental_config(table_config)
    fields = inc.get("fields", [])
    return fields[0] if fields else None


__all__ = [
    "get_incremental_config",
    "resolve_incremental_dates",
    "get_primary_incremental_field",
]
