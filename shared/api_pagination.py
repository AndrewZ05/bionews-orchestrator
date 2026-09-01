"""
Generic API pagination helpers.

Replaces the independent pagination implementations in:
- onetrust_extractor.py (offset-based, ~55 lines)
- threads_extractor.py (cursor-based, ~40 lines)
- npi_registry_extractor.py (offset-based, ~45 lines)

Supports both cursor-based and offset/page-based pagination with optional
rate limiting.
"""

import logging
from typing import Any, Callable, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


def paginate_offset(
    fetch_fn: Callable[..., Any],
    *,
    page_size: int = 100,
    max_pages: Optional[int] = None,
    data_key: str = 'content',
    data_key_fallbacks: Optional[List[str]] = None,
    total_pages_key: str = 'totalPages',
    is_last_key: str = 'last',
    rate_limit_fn: Optional[Callable] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Paginate an API using offset/page-number pagination.

    Args:
        fetch_fn: Callable that accepts ``(page: int, page_size: int)``
            keyword args and returns the parsed JSON response dict.
        page_size: Number of records per page.
        max_pages: Stop after this many pages (``None`` = unlimited).
        data_key: Primary key in response containing the records list.
        data_key_fallbacks: Alternative keys to try if *data_key* is absent.
        total_pages_key: Response key for total page count.
        is_last_key: Response key indicating last page (boolean).
        rate_limit_fn: Optional callable invoked before each request.

    Yields:
        Individual record dicts.
    """
    fallbacks = data_key_fallbacks or ['data', 'results']
    page = 0

    while True:
        if rate_limit_fn:
            rate_limit_fn()

        response = fetch_fn(page=page, page_size=page_size)

        # Extract records from response
        records = _extract_records(response, data_key, fallbacks)
        if not records:
            break

        yield from records

        # Check termination conditions
        total_pages = _deep_get(response, total_pages_key)
        is_last = _deep_get(response, is_last_key)

        if is_last or (total_pages is not None and page >= total_pages - 1):
            break
        if max_pages and page >= max_pages - 1:
            logger.info(f"Reached max pages limit ({max_pages})")
            break

        page += 1

    logger.debug(f"Offset pagination complete after {page + 1} pages")


def paginate_cursor(
    fetch_fn: Callable[..., Any],
    *,
    page_size: int = 100,
    max_pages: Optional[int] = None,
    data_key: str = 'data',
    cursor_path: str = 'paging.cursors.after',
    has_next_path: Optional[str] = 'paging.next',
    rate_limit_fn: Optional[Callable] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Paginate an API using cursor-based pagination (e.g., Meta Graph API).

    Args:
        fetch_fn: Callable that accepts ``(after: Optional[str], limit: int)``
            keyword args and returns the parsed JSON response dict.
        page_size: Number of records per request.
        max_pages: Stop after this many pages (``None`` = unlimited).
        data_key: Key in response containing the records list.
        cursor_path: Dot-separated path to the ``after`` cursor value.
        has_next_path: Dot-separated path to check for next page existence.
        rate_limit_fn: Optional callable invoked before each request.

    Yields:
        Individual record dicts.
    """
    after = None
    page = 0

    while True:
        if rate_limit_fn:
            rate_limit_fn()

        response = fetch_fn(after=after, limit=page_size)

        records = response.get(data_key, [])
        if not records:
            break

        yield from records

        # Get next cursor
        after = _deep_get(response, cursor_path)
        has_next = _deep_get(response, has_next_path) if has_next_path else after

        if not has_next or not after:
            break
        if max_pages and page >= max_pages - 1:
            logger.info(f"Reached max pages limit ({max_pages})")
            break

        page += 1

    logger.debug(f"Cursor pagination complete after {page + 1} pages")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_records(
    response: Any,
    primary_key: str,
    fallback_keys: List[str],
) -> List[Dict[str, Any]]:
    """Extract the records list from a response, trying multiple keys."""
    if isinstance(response, list):
        return response

    if not isinstance(response, dict):
        return []

    content = response.get(primary_key)
    if isinstance(content, list):
        return content

    for key in fallback_keys:
        content = response.get(key)
        if isinstance(content, list):
            return content

    return []


def _deep_get(obj: Any, path: str) -> Any:
    """Get a nested value using dot-separated path (e.g., ``paging.cursors.after``)."""
    if not path or not isinstance(obj, dict):
        return None
    parts = path.split('.')
    current = obj
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


__all__ = [
    'paginate_offset',
    'paginate_cursor',
]
