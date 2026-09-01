"""Threads API HTTP client with rate limiting, pagination, and retry logic.

The facebook_business SDK does NOT support the Threads API.
Threads uses a completely separate domain (graph.threads.net/v1.0),
separate OAuth flow (th_exchange_token / th_refresh_token), and
different grant types. Raw HTTP via requests is required.
"""

import logging
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, List, Any, Iterator, Optional, Tuple
from datetime import date, datetime, timedelta, timezone

from shared.rate_limiter import (
    get_rate_limiter,
    handle_rate_limit_error,
    reset_rate_limit,
    wait_if_needed,
)

logger = logging.getLogger(__name__)

# Threads API base URL — completely separate from graph.facebook.com
THREADS_API_BASE = "https://graph.threads.net/v1.0"

# Rate limits for Threads API
THREADS_RATE_LIMITS: Dict[str, int] = {
    "default": 200,
    "insights": 100,
    "posts": 150,
}


def create_threads_session(timeout: int = 30) -> requests.Session:
    """Create a requests.Session with retry logic for Threads API."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.timeout = timeout
    return session


def wait_for_threads(endpoint_type: str = "default") -> float:
    """Apply rate limiting for Threads API calls."""
    calls_per_hour = THREADS_RATE_LIMITS.get(endpoint_type, 200)
    delay = 3600.0 / calls_per_hour
    time.sleep(max(delay, 1.5))
    return delay


def threads_api_request(
    session: requests.Session,
    endpoint: str,
    access_token: str,
    params: Optional[Dict[str, Any]] = None,
    endpoint_type: str = "default",
    max_retries: int = 3,
) -> Dict[str, Any]:
    """Execute a single GET request against the Threads API.

    Args:
        session: requests.Session with retry config
        endpoint: API path (e.g., '/me' or '/{user_id}/threads')
        access_token: Threads OAuth access token
        params: Additional query parameters
        endpoint_type: For rate limiting bucket selection
        max_retries: Max retry attempts for transient errors
    """
    wait_for_threads(endpoint_type)

    url = f"{THREADS_API_BASE}/{endpoint.lstrip('/')}" if not endpoint.startswith("http") else endpoint
    request_params = {"access_token": access_token}
    if params:
        request_params.update(params)

    for attempt in range(max_retries):
        try:
            response = session.get(url, params=request_params)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Threads rate limit hit. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            if response.status_code in (500, 502, 503, 504) and attempt < max_retries - 1:
                wait_time = 2 ** (attempt + 1)
                logger.warning(f"Threads API error {response.status_code}, retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            raise
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)

    return {}


def paginate_threads_api(
    session: requests.Session,
    endpoint: str,
    access_token: str,
    params: Optional[Dict[str, Any]] = None,
    endpoint_type: str = "default",
    limit: Optional[int] = None,
) -> Iterator[Dict[str, Any]]:
    """Paginate through Threads API cursor-based results.

    Threads uses standard Meta cursor pagination:
    {data: [...], paging: {cursors: {before, after}, next: url}}
    """
    total_yielded = 0
    url = f"{THREADS_API_BASE}/{endpoint.lstrip('/')}"
    request_params = {"access_token": access_token}
    if params:
        request_params.update(params)

    while url:
        data = threads_api_request(session, url, access_token, request_params if not url.startswith("http") else None, endpoint_type)

        for item in data.get("data", []):
            yield item
            total_yielded += 1
            if limit and total_yielded >= limit:
                return

        # Follow cursor-based pagination
        paging = data.get("paging", {})
        next_url = paging.get("next")
        if next_url:
            # Next URL already contains all params including access_token
            url = next_url
            request_params = None  # URL already has params
        else:
            break


def refresh_threads_token(
    session: requests.Session,
    access_token: str,
) -> Optional[str]:
    """Refresh a long-lived Threads access token.

    Long-lived tokens expire after ~60 days. This refreshes them.
    Only valid for tokens that are at least 24 hours old.
    """
    try:
        response = session.get(
            f"{THREADS_API_BASE}/refresh_access_token",
            params={
                "grant_type": "th_refresh_token",
                "access_token": access_token,
            },
        )
        response.raise_for_status()
        data = response.json()
        new_token = data.get("access_token")
        expires_in = data.get("expires_in", 0)
        logger.info(f"Threads token refreshed. Expires in {expires_in // 86400} days.")
        return new_token
    except Exception as e:
        logger.warning(f"Failed to refresh Threads token: {e}")
        return None
