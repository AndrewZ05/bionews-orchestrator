#!/usr/bin/env python3
"""
Standardized Error Handling Framework
======================================

Provides consistent error handling patterns across the codebase.

Features:
- Custom exception hierarchy
- Error context management
- Retry decorators with exponential backoff
- Structured error logging
- Error message templates

Author: Data Pipeline Team
"""

import logging
import time
import random
import threading
from typing import Dict, Any, Optional, Callable, List, Tuple
from functools import wraps
from datetime import datetime

logger = logging.getLogger(__name__)

#
# Custom Exception Hierarchy
#

class PipelineError(Exception):
    # Base exception for all pipeline errors.

    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.context = context or {}
        self.timestamp = datetime.now()

    def __str__(self):
        if self.context:
            context_str = ", ".join([f"{k}={v}" for k, v in self.context.items()])
            return f"{self.message} (Context: {context_str})"
        return self.message


class TransientError(PipelineError):
    # Error that is likely temporary and can be retried.
    pass


class PermanentError(PipelineError):
    # Error that is permanent and should not be retried.
    pass


class ValidationError(PipelineError):
    # Error in data validation.
    pass


class ConfigurationError(PipelineError):
    # Error in configuration.
    pass


class APIError(PipelineError):
    # Error calling external API.

    def __init__(self, message: str, status_code: Optional[int] = None,
                 context: Optional[Dict[str, Any]] = None):
        super().__init__(message, context)
        self.status_code = status_code

    def is_retryable(self) -> bool:
        # Check if error is retryable based on status code.
        if self.status_code is None:
            return False
        # Retry on 5xx errors and 429 (rate limit)
        return self.status_code >= 500 or self.status_code == 429


class SchemaError(PipelineError):
    # Error in schema definition or validation.
    pass


class DataQualityError(PipelineError):
    # Error in data quality checks.
    pass


#
# Error Context Management
#

# Thread-local storage for error context
_error_context = threading.local()

def set_error_context(**kwargs) -> None:
    """
    Set error context for current thread.

    Usage:
        set_error_context(source='facebook', table='posts', site='123')
    """
    if not hasattr(_error_context, 'data'):
        _error_context.data = {}
    _error_context.data.update(kwargs)


def get_error_context() -> Dict[str, Any]:
    # Get error context for current thread.
    if not hasattr(_error_context, 'data'):
        return {}
    return _error_context.data.copy()


def clear_error_context() -> None:
    # Clear error context for current thread.
    if hasattr(_error_context, 'data'):
        _error_context.data.clear()


def with_context(**context_kwargs):
    """
    Decorator to add context to errors raised by function.

    Usage:
        @with_context(source='facebook')
        def extract_data():
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Save old context
            old_context = get_error_context()

            # Set new context
            set_error_context(**context_kwargs)

            try:
                return func(*args, **kwargs)
            finally:
                # Restore old context
                clear_error_context()
                set_error_context(**old_context)

        return wrapper
    return decorator


#
# Retry Logic with Exponential Backoff
#

def retry_on_transient_error(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    exceptions: Tuple = (TransientError, APIError)
):
    """
    Decorator to retry function on transient errors with exponential backoff.

    Args:
        max_attempts: Maximum number of attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential backoff (2.0 = double each time)
        jitter: Add random jitter to prevent thundering herd
        exceptions: Tuple of exception classes to retry on

    Usage:
        @retry_on_transient_error(max_attempts=3)
        def call_api():
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 1

            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    # Check if API error is retryable
                    if isinstance(e, APIError) and not e.is_retryable():
                        logger.error(f"Non-retryable API error in {func.__name__}: {e}")
                        raise

                    if attempt >= max_attempts:
                        logger.error(f"Max retry attempts ({max_attempts}) reached for {func.__name__}: {e}")
                        raise

                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (exponential_base ** (attempt - 1)), max_delay)

                    # Add jitter if enabled
                    if jitter:
                        jitter_amount = delay * 0.1  # 10% jitter
                        delay = delay + random.uniform(-jitter_amount, jitter_amount)

                    logger.warning(
                        f"Transient error in {func.__name__} (attempt {attempt}/{max_attempts}): {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )

                    time.sleep(delay)
                    attempt += 1

        return wrapper
    return decorator


#
# Error Message Templates
#

def format_error_message(
    operation: str,
    error: Exception,
    context: Optional[Dict[str, Any]] = None,
    suggestion: Optional[str] = None
) -> str:
    """
    Format error message with consistent structure.

    Args:
        operation: Description of operation that failed
        error: The exception that occurred
        context: Additional context (table, site, etc.)
        suggestion: Troubleshooting suggestion

    Returns:
        Formatted error message
    """
    # Get error context if not provided
    if context is None:
        context = get_error_context()

    parts = [f"ERROR: {operation} failed"]

    # Add context
    if context:
        context_parts = [f"{k}={v}" for k, v in context.items()]
        parts.append(f"Context: {', '.join(context_parts)}")

    # Add error details
    error_type = type(error).__name__
    parts.append(f"Error: [{error_type}] {str(error)}")

    # Add suggestion if provided
    if suggestion:
        parts.append(f"Suggestion: {suggestion}")

    return "\n  ".join(parts)


def log_error(
    operation: str,
    error: Exception,
    context: Optional[Dict[str, Any]] = None,
    suggestion: Optional[str] = None,
    level: str = "error"
) -> None:
    """
    Log error with consistent formatting.

    Args:
        operation: Description of operation that failed
        error: The exception that occurred
        context: Additional context
        suggestion: Troubleshooting suggestion
        level: Log level (error, warning, critical)
    """
    message = format_error_message(operation, error, context, suggestion)

    log_func = getattr(logger, level, logger.error)
    log_func(message)


#
# Error Recovery Helpers
#

def safe_execute(
    func: Callable,
    default_value: Any = None,
    log_errors: bool = True,
    operation_name: Optional[str] = None
) -> Any:
    """
    Safely execute function and return default value on error.

    Args:
        func: Function to execute
        default_value: Value to return on error
        log_errors: Whether to log errors
        operation_name: Name of operation for logging

    Returns:
        Function result or default value
    """
    try:
        return func()
    except Exception as e:
        if log_errors:
            op_name = operation_name or func.__name__
            log_error(f"Safe execution of {op_name}", e)
        return default_value


def try_functions(
    functions: List[Callable],
    operation_name: str = "fallback execution"
) -> Optional[Any]:
    """
    Try functions in order until one succeeds.

    Args:
        functions: List of functions to try
        operation_name: Name of operation for logging

    Returns:
        Result from first successful function, or None
    """
    for i, func in enumerate(functions, 1):
        try:
            return func()
        except Exception as e:
            logger.debug(f"Attempt {i}/{len(functions)} failed for {operation_name}: {e}")
            if i == len(functions):
                logger.error(f"All {len(functions)} attempts failed for {operation_name}")

    return None


#
# Error Statistics
#

# Thread-safe error tracking
_error_stats = {}
_error_stats_lock = threading.Lock()

def track_error(error_type: str, operation: str) -> None:
    """
    Track error occurrence for monitoring.

    Args:
        error_type: Type of error
        operation: Operation that failed
    """
    with _error_stats_lock:
        key = f"{operation}:{error_type}"
        if key not in _error_stats:
            _error_stats[key] = {
                'count': 0,
                'first_seen': datetime.now(),
                'last_seen': datetime.now()
            }

        _error_stats[key]['count'] += 1
        _error_stats[key]['last_seen'] = datetime.now()


def get_error_stats() -> Dict[str, Any]:
    # Get error statistics.
    with _error_stats_lock:
        return {
            k: {
                'count': v['count'],
                'first_seen': v['first_seen'].isoformat(),
                'last_seen': v['last_seen'].isoformat()
            }
            for k, v in _error_stats.items()
        }


def clear_error_stats() -> None:
    # Clear error statistics.
    with _error_stats_lock:
        _error_stats.clear()


#
# Validation Helper
#

def validate_required_fields(data: Dict[str, Any], required_fields: List[str]) -> None:
    """
    Validate that required fields are present.

    Args:
        data: Data dictionary to validate
        required_fields: List of required field names

    Raises:
        ValidationError: If required fields are missing
    """
    missing = [field for field in required_fields if field not in data or data[field] is None]

    if missing:
        raise ValidationError(
            f"Missing required fields: {', '.join(missing)}",
            context={'provided_fields': list(data.keys())}
        )
