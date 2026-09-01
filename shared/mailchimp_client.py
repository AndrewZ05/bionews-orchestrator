"""Mailchimp API client helpers.

Centralizes HTTP session creation, request handling, pagination, and batch
execution logic so extractors can reuse the same resilient primitives without
duplicated boilerplate.
"""

from __future__ import annotations

import io
import json
import logging
import tarfile
import threading
import time
import zipfile
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from shared.rate_limiter import get_rate_limiter, wait_if_needed

logger = logging.getLogger(__name__)


DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_BATCH_POLL_INTERVAL_SECONDS = 10
DEFAULT_BATCH_TIMEOUT_SECONDS = 3600
# Operations per batch job. Each chunk costs a fixed submit -> poll(10s) ->
# archive-generate -> download round trip regardless of how many rows come
# back, and chunks run sequentially, so wall-clock tracks chunk COUNT far more
# than row count (a 6.6M-row run took 116 min; a 599K-row run took 214 min).
# 200 meant ~5 serial chunks per table, ~30 across campaign_group. Mailchimp
# imposes no documented cap here -- verified it accepts and processes 1000 ops
# in a single job -- so larger chunks trade more per-job preprocessing for far
# fewer round trips. Lower this toward 500 if archive generation starts pushing
# DEFAULT_BATCH_TIMEOUT_SECONDS.
MAX_BATCH_OPERATIONS = 1000
BATCH_FAILURE_STATES = {"failed", "errored", "error", "cancelled", "canceled"}
DEFAULT_BATCH_RESULT_TIMEOUT_SECONDS = 120
# Additional timeout buffer for batch result download after job completes
# Increased to 15 minutes as Mailchimp can be slow to generate result archives for large batches
BATCH_RESULT_DOWNLOAD_GRACE_PERIOD = 900

# Mailchimp enforces 10 req/sec; we use 9 for safety
MAILCHIMP_RATE_LIMITER = "mailchimp_api"
MAILCHIMP_MAX_CONCURRENT_REQUESTS = 20


_rate_limit_update_lock = threading.Lock()
_MAILCHIMP_LIMITER = get_rate_limiter(MAILCHIMP_RATE_LIMITER, calls_per_second=9.0)
with _rate_limit_update_lock:
    calls_per_second = 9.0
    _MAILCHIMP_LIMITER["max_tokens"] = calls_per_second
    _MAILCHIMP_LIMITER["token_rate"] = calls_per_second
    _MAILCHIMP_LIMITER["tokens"] = calls_per_second
    _MAILCHIMP_LIMITER["min_interval"] = 1.0 / calls_per_second

_REQUEST_SEMAPHORE = threading.Semaphore(MAILCHIMP_MAX_CONCURRENT_REQUESTS)


def configure_mailchimp_rate_limit(requests_per_second: float) -> None:
    """Update the shared Mailchimp rate limiter at runtime."""

    if requests_per_second <= 0:
        requests_per_second = 1.0

    limiter = get_rate_limiter(
        MAILCHIMP_RATE_LIMITER, calls_per_second=requests_per_second
    )
    with _rate_limit_update_lock:
        limiter["max_tokens"] = requests_per_second
        limiter["token_rate"] = requests_per_second
        limiter["tokens"] = min(
            limiter.get("tokens", requests_per_second), requests_per_second
        )
        limiter["min_interval"] = 1.0 / requests_per_second


@contextmanager
def _rate_limited_request_slot() -> Iterable[None]:
    wait_if_needed(MAILCHIMP_RATE_LIMITER)
    _REQUEST_SEMAPHORE.acquire()
    try:
        yield
    finally:
        _REQUEST_SEMAPHORE.release()


class MailchimpBatchResultUnavailable(RuntimeError):
    def __init__(self, status_code: int, url: str):
        super().__init__(f"Mailchimp batch result unavailable (HTTP {status_code})")
        self.status_code = status_code
        self.url = url


def create_mailchimp_session(
    api_key: str, request_timeout: int = DEFAULT_TIMEOUT_SECONDS
) -> requests.Session:
    """Create a configured requests session for Mailchimp API access."""

    session = requests.Session()
    session.auth = ("orchestrator", api_key)
    session.headers.update(
        {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Connection": "keep-alive",
            "Accept-Encoding": "gzip",
        }
    )
    # Mailchimp fronts the API with an istio-envoy proxy that sheds load with
    # 503s for minutes at a time, not seconds. The old policy (total=3,
    # backoff_factor=0.5) gave up after ~3.5s of cumulative wait, so a shedding
    # window killed the whole run on its very first call.
    #
    # This layer also masks mailchimp_request()'s own 5xx/429 backoff below:
    # urllib3 raises RetryError from inside session.request(), which surfaces as
    # a RequestException, so the application loop never sees the status code.
    # Retries here therefore have to carry the real patience.
    #
    # total=6 with backoff_factor=2 waits 0+2+4+8+16+32 = ~62s across attempts,
    # and respect_retry_after_header lets Mailchimp dictate a longer pause when
    # it sends Retry-After. Combined with the caller's own max_attempts loop,
    # a transient shed no longer ends the run.
    adapter = HTTPAdapter(
        pool_connections=20,
        pool_maxsize=20,
        max_retries=Retry(
            total=6,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=None,
            respect_retry_after_header=True,
            raise_on_status=False,
        ),
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.timeout = request_timeout
    return session


def mailchimp_request(
    session: requests.Session,
    base_url: str,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    max_attempts: int = 3,
    backoff_seconds: float = 2.0,
) -> Dict[str, Any]:
    """Execute a Mailchimp API request with retry/backoff semantics."""

    url = f"{base_url}{path}"
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            with _rate_limited_request_slot():
                response = session.request(
                    method, url, params=params, timeout=session.timeout
                )
        except requests.RequestException as exc:
            if attempt < max_attempts:
                delay = backoff_seconds * (2 ** (attempt - 1))
                time.sleep(delay)
                continue
            raise RuntimeError(f"Mailchimp request error for {path}: {exc}") from exc

        status = response.status_code
        if status == 429:
            if attempt < max_attempts:
                delay = backoff_seconds * (2 ** (attempt - 1))
                time.sleep(delay)
                continue
            raise RuntimeError(
                f"Mailchimp rate limit exceeded for {path} after {max_attempts} attempts"
            )

        if status >= 500:
            if attempt < max_attempts:
                delay = backoff_seconds * (2 ** (attempt - 1))
                time.sleep(delay)
                continue
            raise RuntimeError(
                f"Mailchimp server error ({status}) for {path}: {response.text[:200]}"
            )

        if status >= 400:
            raise RuntimeError(
                f"Mailchimp request failed ({status}) for {path}: {response.text[:200]}"
            )

        if not response.content:
            return {}

        try:
            return response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON response for {path}: {exc}")

    raise RuntimeError(
        f"Mailchimp request failed for {path} after {max_attempts} attempts"
    )


def fetch_paginated_collection(
    session: requests.Session,
    base_url: str,
    path: str,
    data_key: str,
    params: Optional[Dict[str, Any]] = None,
    page_size: int = 500,
    max_attempts: int = 3,
    backoff_seconds: float = 2.0,
    progress_logger: Optional[Any] = None,
    progress_interval: int = 10000,
) -> List[Dict[str, Any]]:
    """Iterate a Mailchimp paginated REST collection."""

    records: List[Dict[str, Any]] = []
    offset = 0
    params = params.copy() if params else {}
    last_emit = 0

    while True:
        page_params = params.copy()
        page_params["count"] = page_size
        page_params["offset"] = offset

        payload = mailchimp_request(
            session, base_url, "GET", path, page_params, max_attempts, backoff_seconds
        )
        items = payload.get(data_key, []) if isinstance(payload, dict) else []
        if not isinstance(items, list):
            break

        records.extend(items)
        total_items = payload.get("total_items") if isinstance(payload, dict) else None

        if progress_logger and len(records) - last_emit >= progress_interval:
            progress_logger(len(records), total_items)
            last_emit = len(records)

        offset += page_size
        if total_items is not None and offset >= total_items:
            break
        if not items:
            break

    if progress_logger and len(records) > last_emit:
        final_total = payload.get("total_items") if isinstance(payload, dict) else None
        progress_logger(len(records), final_total)

    return records


def build_batch_operation_path(path: str, params: Dict[str, Any]) -> str:
    from urllib.parse import urlencode

    if not params:
        return path
    query = urlencode({k: v for k, v in params.items() if v is not None})
    return f"{path}?{query}"


def build_batch_operation(
    method: str, path: str, params: Dict[str, Any], operation_id: str
) -> Dict[str, Any]:
    # Mailchimp Batch API requires query parameters in a separate `params` field
    # as string values, NOT embedded in the path query string. Embedding them
    # in path causes the batch worker to silently drop them and return default
    # (empty) results.
    op: Dict[str, Any] = {
        "method": method,
        "path": path,
        "operation_id": operation_id,
    }
    if params:
        op["params"] = {k: str(v) for k, v in params.items() if v is not None}
    return op


def _submit_batch_job(
    session: requests.Session,
    base_url: str,
    operations: List[Dict[str, Any]],
    max_attempts: int,
    backoff_seconds: float,
) -> Dict[str, Any]:
    url = f"{base_url}/batches"
    payload = {"operations": operations}
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        with _rate_limited_request_slot():
            response = session.post(
                url,
                json=payload,
                timeout=getattr(session, "timeout", DEFAULT_TIMEOUT_SECONDS),
            )
        status = response.status_code
        if status == 429 and attempt < max_attempts:
            delay = backoff_seconds * (2 ** (attempt - 1))
            time.sleep(delay)
            continue
        if status >= 500 and attempt < max_attempts:
            delay = backoff_seconds * (2 ** (attempt - 1))
            time.sleep(delay)
            continue
        if status >= 400:
            raise RuntimeError(
                f"Mailchimp batch submit failed ({status}): {response.text[:200]}"
            )
        try:
            return response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Invalid JSON response from Mailchimp batch submit: {exc}"
            )

    raise RuntimeError("Failed to submit Mailchimp batch after retries")


def _poll_batch_job(
    session: requests.Session,
    base_url: str,
    batch_id: str,
    poll_interval: int,
    timeout_seconds: int,
) -> Dict[str, Any]:
    url = f"{base_url}/batches/{batch_id}"
    deadline = time.time() + timeout_seconds
    last_state = "unknown"
    while time.time() < deadline:
        with _rate_limited_request_slot():
            response = session.get(
                url, timeout=getattr(session, "timeout", DEFAULT_TIMEOUT_SECONDS)
            )
        status = response.status_code
        if status >= 400:
            raise RuntimeError(
                f"Mailchimp batch status check failed ({status}): {response.text[:200]}"
            )
        payload = response.json()
        raw_state = payload.get("status")
        state = raw_state.lower().strip() if isinstance(raw_state, str) else ""
        if state:
            last_state = state
        if state == "finished":
            return payload

        if state in BATCH_FAILURE_STATES:
            raise RuntimeError(f"Mailchimp batch {batch_id} ended with status {state}")

        time.sleep(poll_interval)
    raise RuntimeError(
        f"Timed out waiting for Mailchimp batch {batch_id} "
        f"after {timeout_seconds}s (last_status={last_state}, poll_interval={poll_interval}s)"
    )


def _fetch_batch_status(
    session: requests.Session, base_url: str, batch_id: str
) -> Dict[str, Any]:
    url = f"{base_url}/batches/{batch_id}"
    with _rate_limited_request_slot():
        response = session.get(
            url, timeout=getattr(session, "timeout", DEFAULT_TIMEOUT_SECONDS)
        )
    if response.status_code >= 400:
        raise RuntimeError(
            f"Mailchimp batch status fetch failed ({response.status_code}): {response.text[:200]}"
        )
    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Invalid JSON response while refreshing batch {batch_id}: {exc}"
        )


def _download_batch_result(session: requests.Session, result_url: str) -> bytes:
    # The result URL is an AWS S3 pre-signed URL with auth in the query string.
    # Sending the session's Basic auth header alongside it triggers an S3
    # "Only one auth mechanism allowed" 400 — so we issue an unauthenticated
    # GET. We still rate-limit through the session's slot to be polite to
    # the API account, and we still respect the session timeout.
    timeout = getattr(session, "timeout", DEFAULT_TIMEOUT_SECONDS)
    with _rate_limited_request_slot():
        response = requests.get(result_url, timeout=timeout)
    if response.status_code >= 400:
        raise MailchimpBatchResultUnavailable(response.status_code, result_url)
    return response.content


def _parse_batch_archive(content: bytes) -> Dict[str, Dict[str, Any]]:
    # Mailchimp's batch result archive is a gzipped tarball. Each file inside
    # contains a JSON ARRAY of per-operation result objects. Each object has
    # the shape {status_code, operation_id, response} where `response` is a
    # JSON-encoded STRING of the operation's response body (Mailchimp encodes
    # it as a string, not nested JSON).
    #
    # We accept fallbacks for resilience: a tarball entry could in theory
    # contain a single object instead of an array, and historical/legacy
    # archives could be zip format. The legacy zip path is kept so that any
    # in-flight batches submitted before this change can still be parsed.
    results: Dict[str, Dict[str, Any]] = {}

    def _ingest(payload: Any) -> None:
        if isinstance(payload, list):
            for entry in payload:
                _ingest(entry)
            return
        if not isinstance(payload, dict):
            return
        op_id = payload.get("operation_id") or payload.get("id")
        if not op_id:
            return
        status_code = payload.get("status_code")
        if status_code is None and isinstance(payload.get("response"), dict):
            status_code = payload["response"].get("status_code")
        response = payload.get("response")
        body: Any
        if isinstance(response, dict):
            body = response.get("body")
        else:
            body = response if response is not None else payload.get("body")
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError:
                pass
        results[str(op_id)] = {"status_code": status_code, "body": body}

    if content[:2] == b"\x1f\x8b":
        with tarfile.open(fileobj=io.BytesIO(content), mode="r:gz") as archive:
            for member in archive.getmembers():
                if not member.isfile():
                    continue
                handle = archive.extractfile(member)
                if handle is None:
                    continue
                try:
                    payload = json.loads(handle.read().decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
                _ingest(payload)
    else:
        with zipfile.ZipFile(io.BytesIO(content), "r") as archive:
            for name in archive.namelist():
                with archive.open(name) as handle:
                    try:
                        payload = json.loads(handle.read().decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
                _ingest(payload)
    return results


def _execute_batch_chunk(
    session: requests.Session,
    base_url: str,
    operations: List[Dict[str, Any]],
    max_attempts: int,
    backoff_seconds: float,
    poll_interval: int,
    timeout_seconds: int,
    label: Optional[str] = None,
    chunk_label: Optional[str] = None,
) -> List[Dict[str, Any]]:
    submission = _submit_batch_job(
        session, base_url, operations, max_attempts, backoff_seconds
    )
    batch_id = submission.get("id")
    if not batch_id:
        raise RuntimeError("Mailchimp batch submission returned no batch id")
    logger.info(
        "[batch] %s%s submitted batch_id=%s ops=%d timeout=%ds",
        label or "unlabeled",
        f" {chunk_label}" if chunk_label else "",
        batch_id,
        len(operations),
        timeout_seconds,
    )
    try:
        status_payload = _poll_batch_job(
            session, base_url, batch_id, poll_interval, timeout_seconds
        )
    except RuntimeError as exc:
        logger.error(
            "[batch] %s%s batch_id=%s failed: %s",
            label or "unlabeled",
            f" {chunk_label}" if chunk_label else "",
            batch_id,
            exc,
        )
        raise
    result_url = status_payload.get("response_body_url")
    # Extended deadline: original timeout + grace period for result download
    result_deadline = time.time() + BATCH_RESULT_DOWNLOAD_GRACE_PERIOD

    # Track when we last logged to reduce chattiness
    last_log_time = 0
    retry_count = 0

    while True:
        if result_url:
            try:
                archive_bytes = _download_batch_result(session, result_url)
                break
            except MailchimpBatchResultUnavailable as unavailable:
                if (
                    unavailable.status_code
                    in {202, 400, 401, 403, 404, 408, 429, 500, 502, 503, 504}
                    and time.time() < result_deadline
                ):
                    retry_count += 1
                    now = time.time()
                    # Only log every 60 seconds to reduce chattiness
                    if now - last_log_time >= 60 or retry_count == 1:
                        logger.info(
                            "Batch %s result not ready (HTTP %s); retrying (attempt %d, %.0f seconds remaining)",
                            batch_id,
                            unavailable.status_code,
                            retry_count,
                            result_deadline - now,
                        )
                        last_log_time = now
                    time.sleep(poll_interval)
                    status_payload = _fetch_batch_status(session, base_url, batch_id)
                    state = (status_payload.get("status") or "").lower()
                    if state in BATCH_FAILURE_STATES:
                        raise RuntimeError(
                            f"Mailchimp batch {batch_id} ended with status {state}"
                        )
                    result_url = status_payload.get("response_body_url") or result_url
                    continue
                raise RuntimeError(
                    f"Failed to download Mailchimp batch results ({unavailable.status_code})"
                )
        else:
            if time.time() >= result_deadline:
                raise RuntimeError(
                    f"Mailchimp batch {batch_id} finished without response body URL"
                )
            time.sleep(poll_interval)
            status_payload = _fetch_batch_status(session, base_url, batch_id)
            state = (status_payload.get("status") or "").lower()
            if state in BATCH_FAILURE_STATES:
                raise RuntimeError(
                    f"Mailchimp batch {batch_id} ended with status {state}"
                )
            result_url = status_payload.get("response_body_url")

    parsed = _parse_batch_archive(archive_bytes)
    responses: List[Dict[str, Any]] = []
    for op in operations:
        op_id = str(op["operation_id"])
        if op_id not in parsed:
            raise RuntimeError(f"Missing batch result for operation {op_id}")
        responses.append(parsed[op_id])
    return responses


def execute_mailchimp_batch(
    session: requests.Session,
    base_url: str,
    operations: List[Dict[str, Any]],
    max_attempts: int,
    backoff_seconds: float,
    poll_interval: int = DEFAULT_BATCH_POLL_INTERVAL_SECONDS,
    timeout_seconds: int = DEFAULT_BATCH_TIMEOUT_SECONDS,
    label: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Execute multiple Mailchimp operations using the batch API.

    label: optional caller-supplied context (e.g. "campaign_sent_to") that
    is included in batch submit/timeout log lines so failures are
    diagnosable without cross-referencing batch IDs.
    """

    if not operations:
        return []

    total_chunks = (len(operations) + MAX_BATCH_OPERATIONS - 1) // MAX_BATCH_OPERATIONS
    responses: List[Dict[str, Any]] = []
    for idx in range(0, len(operations), MAX_BATCH_OPERATIONS):
        chunk = operations[idx : idx + MAX_BATCH_OPERATIONS]
        chunk_label = (
            f"chunk {idx // MAX_BATCH_OPERATIONS + 1}/{total_chunks}"
            if total_chunks > 1
            else None
        )
        responses.extend(
            _execute_batch_chunk(
                session,
                base_url,
                chunk,
                max_attempts,
                backoff_seconds,
                poll_interval,
                timeout_seconds,
                label=label,
                chunk_label=chunk_label,
            )
        )
    return responses


def batch_fetch_paginated_collection(
    session: requests.Session,
    base_url: str,
    path: str,
    data_key: str,
    params: Optional[Dict[str, Any]] = None,
    page_size: int = 500,
    max_attempts: int = 3,
    backoff_seconds: float = 2.0,
    poll_interval: int = DEFAULT_BATCH_POLL_INTERVAL_SECONDS,
    timeout_seconds: int = DEFAULT_BATCH_TIMEOUT_SECONDS,
    progress_logger: Optional[Any] = None,
    progress_interval: int = 10000,
) -> List[Dict[str, Any]]:
    """Fetch a paginated collection using the Mailchimp batch API."""

    params = params.copy() if params else {}
    params["count"] = page_size
    params["offset"] = 0

    initial_payload = mailchimp_request(
        session, base_url, "GET", path, params, max_attempts, backoff_seconds
    )
    items = (
        initial_payload.get(data_key, []) if isinstance(initial_payload, dict) else []
    )
    total_items = (
        initial_payload.get("total_items")
        if isinstance(initial_payload, dict)
        else None
    )
    records: List[Dict[str, Any]] = list(items)

    if progress_logger:
        progress_logger(len(records), total_items)

    if not records and total_items is None:
        return records

    if total_items is None:
        raise RuntimeError(
            f"Batch pagination for {path} requires total_items; falling back to REST"
        )

    operations: List[Dict[str, Any]] = []
    for offset in range(page_size, total_items, page_size):
        page_params = params.copy()
        page_params["offset"] = offset
        operation_id = f"{path}:{offset}"
        operations.append(build_batch_operation("GET", path, page_params, operation_id))

    if not operations:
        return records

    batch_responses = execute_mailchimp_batch(
        session,
        base_url,
        operations,
        max_attempts,
        backoff_seconds,
        poll_interval,
        timeout_seconds,
    )

    for response in batch_responses:
        status_code = response.get("status_code", 200)
        if status_code and status_code >= 400:
            raise RuntimeError(
                f"Mailchimp batch response returned error status {status_code}"
            )
        payload = response.get("body")
        if isinstance(payload, dict):
            page_items = payload.get(data_key, [])
            if isinstance(page_items, list):
                records.extend(page_items)
        if progress_logger and len(records) >= progress_interval:
            progress_logger(len(records), total_items)

    if progress_logger:
        progress_logger(len(records), total_items)

    return records


# Ensure rate limiter initialized for modules that rely on it
get_rate_limiter(MAILCHIMP_RATE_LIMITER, calls_per_second=9.0)
