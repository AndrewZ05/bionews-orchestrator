"""
Facebook error classification -- pure, SDK-free helpers.

Extracted from plugins/facebook_extractor so the rate-limit classifier can be
unit-tested without importing facebook_business (which is absent in some envs).

Design rule (the hardening): the STRUCTURED Facebook error code is authoritative.
When a code is present, message text must NOT be able to override it -- a
code-100 "invalid metric" error whose text merely mentions "rate limit" must not
be retried as a rate limit (the misclassification class fixed in api_retry).
Text is consulted ONLY when no structured code is present.
"""

from typing import Any, Optional

# App rate limit (4), user request limit (17), account rate limit (80004).
RATE_LIMIT_CODES = {4, 17, 80004}
RATE_LIMIT_TEXT = (
    "user request limit reached",
    "application request limit reached",
    "request limit reached",
    "too many calls",
    "too many api requests",
    "rate limit",
)


def extract_error_code(error: Any) -> Optional[int]:
    """Return the Facebook structured error code, or None if absent/unavailable."""
    try:
        code = error.api_error_code()  # facebook_business FacebookRequestError
    except Exception:
        return None
    return code


def classify_rate_limit(code: Optional[int], message: str) -> bool:
    """
    True if this represents a recoverable Facebook rate limit.

    Structured code wins: if `code` is not None it is the sole authority. Only
    when `code` is None do we fall back to message-text patterns.
    """
    if code is not None:
        return code in RATE_LIMIT_CODES
    text = (message or "").lower()
    return any(token in text for token in RATE_LIMIT_TEXT)


def is_facebook_rate_limit(error: Any) -> bool:
    """Classify an exception as a recoverable Facebook rate-limit (code-first)."""
    return classify_rate_limit(extract_error_code(error), str(error))
