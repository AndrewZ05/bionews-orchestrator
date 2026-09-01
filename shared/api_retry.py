#!/usr/bin/env python3
"""
Generic API Retry Logic
=======================

Provides source-agnostic retry decorators and error handling for API calls.

This module handles:
- Transient errors (temporary failures)
- Rate limiting with progressive backoff
- Configurable retry strategies
- Error type detection and classification

Can be used with any API (Facebook, WordPress, generic REST, etc.)

Usage:
    from shared.api_retry import with_api_retry, is_rate_limit_error

    @with_api_retry(
        resource_name='campaigns',
        max_retries=6,
        backoff_strategy='progressive'
    )
    def fetch_campaigns(account_id):
        # Your API call here
        return api.get_campaigns(account_id)

Author: Data Pipeline Team
Created: 2025-01-20
"""

import time
import logging
from typing import Callable, Optional, Dict, Any, List
from functools import wraps

logger = logging.getLogger(__name__)


# Error detection patterns (source-agnostic). NOTE: HTTP status codes are NOT
# listed here as bare digit substrings -- "500"/"429" etc. would match any
# fbtrace_id / object id / param value that happens to contain those digits,
# which is exactly how code-100 errors got misclassified as transient/rate-limit.
# HTTP status codes are matched separately as proper tokens (see _has_http_status).
TRANSIENT_ERROR_PATTERNS = [
    "is_transient",
    "temporary",
    "timeout",
    "timed out",
    "connection reset",
    "connection refused",
    "service unavailable",
]
TRANSIENT_HTTP_STATUSES = (500, 502, 503, 504)

RATE_LIMIT_ERROR_PATTERNS = [
    "rate limit",
    "too many requests",
    "too many calls",
    "quota exceeded",
    "throttled",
]
RATE_LIMIT_HTTP_STATUSES = (429,)

# Error codes that indicate transient issues
TRANSIENT_ERROR_CODES = {
    2,  # Facebook: Temporary issue
    17,  # Facebook: User request limit reached
    80000,  # Facebook: Server error
    80004,  # Facebook: Service temporarily unavailable
}

# Error codes that indicate rate limiting
RATE_LIMIT_ERROR_CODES = {
    4,  # Facebook: Application request limit reached
    17,  # Facebook: User request limit reached
    32,  # Facebook: Page request limit reached
    80004,  # Facebook: Throttling
    429,  # HTTP: Too Many Requests
}

# Error codes that are PERMANENT -- retrying cannot succeed. These must
# short-circuit retry classification so a config/parameter error does not get
# misclassified as transient/rate-limited and burn the full backoff schedule.
# Facebook code 100 (OAuthException "invalid parameter", e.g. a deprecated
# insights metric) is the motivating case: a 7-day facebook run wasted hours
# retrying code-100 errors 3x each (30/60/120s) because the error text happened
# to contain digit substrings ("429"/"500") that matched the bare-substring
# patterns below. A permanent error must fail fast on attempt 1.
NON_RETRYABLE_ERROR_CODES = {
    100,  # Facebook: Invalid parameter (e.g. deprecated/unknown insights metric)
    190,  # Facebook: Invalid/expired access token
    200,  # Facebook: Permissions error
    803,  # Facebook: Object does not exist / cannot be loaded
}


def _has_error_code(error_str: str, code: int) -> bool:
    """True if `error_str` (already lowercased) reports Facebook/HTTP `code`."""
    return (
        f'"code": {code}' in error_str
        or f'"code":{code}' in error_str
        or f"error code {code}" in error_str
        or f"error_code: {code}" in error_str
    )


def _has_http_status(error_str: str, status: int) -> bool:
    """
    True if `error_str` (already lowercased) reports HTTP `status` as a STATUS
    token -- not a bare digit substring. Avoids matching the digits inside an
    fbtrace_id / object id / param value (the bare-substring false positives).
    """
    s = str(status)
    return (
        f"status: {s}" in error_str
        or f"status:{s}" in error_str
        or f"status code {s}" in error_str
        or f"http {s}" in error_str
        or f"({s})" in error_str  # e.g. "(429)"
    )


def is_non_retryable_error(error: Exception) -> bool:
    """
    True for PERMANENT errors where retrying cannot help (invalid parameter,
    bad token, permissions). Checked BEFORE transient/rate-limit classification
    so a config error fails fast instead of running the full backoff schedule.
    """
    error_str = str(error).lower()
    return any(_has_error_code(error_str, code) for code in NON_RETRYABLE_ERROR_CODES)


def is_transient_error(error: Exception) -> bool:
    """
    Check if error is a transient error that should be retried.

    A permanent error (NON_RETRYABLE_ERROR_CODES) is never transient.
    """
    if is_non_retryable_error(error):
        return False

    error_str = str(error).lower()

    if any(pattern in error_str for pattern in TRANSIENT_ERROR_PATTERNS):
        return True
    if any(_has_http_status(error_str, s) for s in TRANSIENT_HTTP_STATUSES):
        return True
    if any(_has_error_code(error_str, code) for code in TRANSIENT_ERROR_CODES):
        return True

    return False


def is_rate_limit_error(error: Exception) -> bool:
    """
    Check if error is a rate limit error.

    A permanent error (NON_RETRYABLE_ERROR_CODES) is never a rate limit.
    """
    if is_non_retryable_error(error):
        return False

    error_str = str(error).lower()

    if any(pattern in error_str for pattern in RATE_LIMIT_ERROR_PATTERNS):
        return True
    if any(_has_http_status(error_str, s) for s in RATE_LIMIT_HTTP_STATUSES):
        return True
    if any(_has_error_code(error_str, code) for code in RATE_LIMIT_ERROR_CODES):
        return True

    return False


def calculate_backoff_delay(
    retry_count: int,
    strategy: str = "exponential",
    base_delay: float = 30.0,
    max_delay: float = 900.0,
) -> float:
    """
    Calculate backoff delay for retry attempts.

    Args:
        retry_count: Current retry attempt number (1-based)
        strategy: Backoff strategy ('exponential', 'linear', 'progressive', 'fibonacci')
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds

    Returns:
        Delay in seconds
    """
    if strategy == "exponential":
        # Exponential: 30s, 60s, 120s, 240s, 480s, 900s
        delay = base_delay * (2 ** (retry_count - 1))

    elif strategy == "linear":
        # Linear: 30s, 60s, 90s, 120s, 150s, 180s
        delay = base_delay * retry_count

    elif strategy == "progressive":
        # Progressive (Facebook pattern): 300s, 600s, 900s, 1080s, 1260s, 1440s
        if retry_count <= 2:
            delay = 300 * retry_count  # 5min, 10min
        else:
            delay = 900 + (300 * (retry_count - 2))  # 15min, 18min, 21min, 24min

    elif strategy == "fibonacci":
        # Fibonacci: 30s, 30s, 60s, 90s, 150s, 240s
        if retry_count <= 2:
            delay = base_delay
        else:
            # Calculate fibonacci-like sequence
            fib = [1, 1]
            for i in range(2, retry_count):
                fib.append(fib[-1] + fib[-2])
            delay = base_delay * fib[-1]
    else:
        # Default to exponential
        delay = base_delay * (2 ** (retry_count - 1))

    # Cap at max_delay
    return min(delay, max_delay)


def with_api_retry(
    resource_name: str = "default",
    max_retries: int = 6,
    backoff_strategy: str = "exponential",
    base_delay: float = 30.0,
    max_delay: float = 900.0,
    retry_on_transient: bool = True,
    retry_on_rate_limit: bool = True,
    pre_retry_wait: Optional[Callable[[int, str], None]] = None,
    post_retry_callback: Optional[Callable[[int, str, Exception], None]] = None,
):
    """
    Decorator for API calls with intelligent retry logic.

    Handles:
    - Rate limiting with configurable backoff
    - Transient errors with exponential backoff
    - Progressive delays for rate limits
    - Custom pre-retry and post-retry callbacks

    Args:
        resource_name: Name of resource being accessed (for logging)
        max_retries: Maximum number of retry attempts
        backoff_strategy: Strategy for calculating delays
            - 'exponential': 2^n growth (default)
            - 'linear': Linear growth
            - 'progressive': Facebook-style progressive (5→10→15→18→21→24 min)
            - 'fibonacci': Fibonacci sequence
        base_delay: Base delay in seconds for backoff calculation
        max_delay: Maximum delay cap in seconds
        retry_on_transient: Whether to retry on transient errors
        retry_on_rate_limit: Whether to retry on rate limit errors
        pre_retry_wait: Optional callback before waiting (e.g., for rate limiter check)
            Signature: (retry_count: int, resource_name: str) -> None
        post_retry_callback: Optional callback after retry
            Signature: (retry_count: int, resource_name: str, error: Exception) -> None

    Returns:
        Decorator function

    Example:
        @with_api_retry(
            resource_name='facebook_campaigns',
            max_retries=5,
            backoff_strategy='exponential'
        )
        def fetch_data():
            return api.get('/campaigns')
    """

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_count = 0
            last_error = None

            while retry_count <= max_retries:
                # Pre-retry wait (e.g., check rate limiter)
                if pre_retry_wait and retry_count > 0:
                    pre_retry_wait(retry_count, resource_name)

                try:
                    # Execute the function
                    result = func(*args, **kwargs)

                    # Success - log if this was a retry
                    if retry_count > 0:
                        logger.info(
                            f"  [{resource_name}] Success after {retry_count} retries"
                        )

                    return result

                except Exception as e:
                    last_error = e
                    error_str = str(e)

                    # PERMANENT errors (invalid parameter, bad token, permissions)
                    # never retry -- fail fast on attempt 1. Guards against the
                    # bare-digit-substring misclassification that made code-100
                    # errors burn the full 30/60/120s schedule per call.
                    if is_non_retryable_error(e):
                        logger.debug(
                            f"  [{resource_name}] Non-retryable error -- not retrying: "
                            f"{error_str[:200]}"
                        )
                        raise

                    # Classify error
                    is_transient = is_transient_error(e)
                    is_rate_limited = is_rate_limit_error(e)

                    # Determine if we should retry
                    should_retry = (retry_on_transient and is_transient) or (
                        retry_on_rate_limit and is_rate_limited
                    )

                    if should_retry and retry_count < max_retries:
                        retry_count += 1

                        # Calculate delay
                        if is_rate_limited and backoff_strategy == "progressive":
                            # Use progressive strategy for rate limits
                            delay = calculate_backoff_delay(
                                retry_count, "progressive", base_delay, max_delay
                            )
                        else:
                            # Use specified strategy for transient errors
                            delay = calculate_backoff_delay(
                                retry_count, backoff_strategy, base_delay, max_delay
                            )

                        # Log retry attempt
                        error_type = (
                            "rate limit" if is_rate_limited else "transient error"
                        )
                        logger.warning(
                            f"  [{resource_name}] {error_type.capitalize()} detected. "
                            f"Retrying in {delay:.0f}s ({delay/60:.1f} min) "
                            f"(attempt {retry_count}/{max_retries})"
                        )
                        logger.debug(f"  Error details: {error_str[:200]}")

                        # Wait before retry
                        time.sleep(delay)

                        # Post-retry callback
                        if post_retry_callback:
                            post_retry_callback(retry_count, resource_name, e)

                    else:
                        # Non-retryable error or max retries exceeded
                        if retry_count >= max_retries:
                            error_type = (
                                "rate limit" if is_rate_limited else "transient error"
                            )
                            raise Exception(
                                f"[{resource_name}] {error_type.capitalize()} exceeded "
                                f"after {max_retries} retries: {error_str}"
                            )
                        else:
                            # Non-retryable error - re-raise immediately
                            raise

            # Should not reach here, but just in case
            raise Exception(
                f"[{resource_name}] Failed after {max_retries} retries. "
                f"Last error: {str(last_error)}"
            )

        return wrapper

    return decorator


def create_retry_wrapper(
    func: Callable, resource_name: str = "default", **retry_kwargs
) -> Callable:
    """
    Create a retry wrapper for a function without using decorator syntax.

    Useful for wrapping lambda functions or dynamically created functions.

    Args:
        func: Function to wrap
        resource_name: Name of resource for logging
        **retry_kwargs: Additional arguments for with_api_retry

    Returns:
        Wrapped function with retry logic

    Example:
        wrapped_fn = create_retry_wrapper(
            lambda: api.get('/data'),
            resource_name='api_data',
            max_retries=3
        )
        result = wrapped_fn()
    """
    return with_api_retry(resource_name=resource_name, **retry_kwargs)(func)


# Convenience functions for common retry patterns


def with_exponential_backoff(resource_name: str = "default", max_retries: int = 5):
    """Exponential backoff retry (30s → 60s → 120s → 240s → 480s)."""
    return with_api_retry(
        resource_name=resource_name,
        max_retries=max_retries,
        backoff_strategy="exponential",
        base_delay=30.0,
    )


def with_progressive_backoff(resource_name: str = "default", max_retries: int = 6):
    """Progressive backoff retry (5min → 10min → 15min → 18min → 21min → 24min)."""
    return with_api_retry(
        resource_name=resource_name,
        max_retries=max_retries,
        backoff_strategy="progressive",
        base_delay=300.0,
    )


def with_linear_backoff(resource_name: str = "default", max_retries: int = 5):
    """Linear backoff retry (30s → 60s → 90s → 120s → 150s)."""
    return with_api_retry(
        resource_name=resource_name,
        max_retries=max_retries,
        backoff_strategy="linear",
        base_delay=30.0,
    )
