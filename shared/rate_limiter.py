#!/usr/bin/env python3
"""
Enhanced API Rate Limiting with Token Bucket Algorithm
======================================================

Provides comprehensive rate limiting with:
- Token bucket algorithm for smooth rate limiting
- Exponential backoff with jitter for rate limit errors
- Thread-safe operations
- Comprehensive statistics and monitoring
- Decorator support for easy integration

Features:
- Token bucket algorithm for burst handling
- Exponential backoff with jitter
- Thread-safe operations
- Statistics tracking
- Decorator support
"""

import logging
import random
import threading
import uuid
from time import sleep, time
from typing import Dict, Any, Optional, Callable

# Setup logging
logger = logging.getLogger(__name__)

# Rate limiter state
_rate_limiters = {}
_rate_limiter_lock = threading.RLock()  # Use RLock to prevent deadlocks

def get_rate_limiter(source: str, calls_per_second: float = 1.0,
                     max_backoff_seconds: float = 300.0) -> Dict[str, Any]:
    """
    Get or create rate limiter for source using token bucket algorithm.

    Args:
        source: Source identifier for rate limiting
        calls_per_second: Maximum calls per second allowed
        max_backoff_seconds: Cap on exponential backoff (default 5 min).
            For per-hour-quota APIs (e.g. Instagram 200/hr), pass a larger
            value (e.g. 3600) so backoff can outlast quota windows.

    Returns:
        Rate limiter configuration dictionary
    """
    with _rate_limiter_lock:
        if source not in _rate_limiters:
            _rate_limiters[source] = {
                # Token bucket parameters
                'tokens': calls_per_second,  # Start with full bucket
                'max_tokens': calls_per_second,  # Maximum tokens (bucket size)
                'token_rate': calls_per_second,  # Token refill rate per second
                'last_refill': time(),  # Last time tokens were refilled

                # Traditional rate limiting
                'min_interval': 1.0 / calls_per_second,
                'last_call': 0,

                # Backoff parameters
                'backoff_until': 0,
                'consecutive_errors': 0,
                'max_backoff_seconds': max_backoff_seconds,

                # Statistics
                'calls_total': 0,
                'waits_total': 0,
                'wait_time_total': 0,
                'errors_total': 0
            }
        return _rate_limiters[source]

def wait_if_needed(source: str) -> float:
    """
    Wait if rate limit requires, returns wait time in seconds.
    
    Args:
        source: Source identifier
        
    Returns:
        Wait time in seconds
    """
    limiter = get_rate_limiter(source)
    
    now = time()
    wait_time = 0
    
    with _rate_limiter_lock:
        # Check if in backoff period
        if now < limiter['backoff_until']:
            wait_time = limiter['backoff_until'] - now
            limiter['waits_total'] += 1
            limiter['wait_time_total'] += wait_time
            logger.debug(f"Rate limiter {source} in backoff, waiting {wait_time:.2f}s")
        else:
            # Refill tokens based on time passed
            time_passed = now - limiter['last_refill']
            new_tokens = time_passed * limiter['token_rate']
            
            limiter['tokens'] = min(
                limiter['tokens'] + new_tokens,
                limiter['max_tokens']
            )
            limiter['last_refill'] = now
            
            # Check if we have a token
            if limiter['tokens'] < 1:
                # Calculate wait time
                wait_time = (1 - limiter['tokens']) / limiter['token_rate']
                limiter['waits_total'] += 1
                limiter['wait_time_total'] += wait_time
                logger.debug(f"Rate limiter {source} needs {wait_time:.2f}s for token")
            else:
                # Consume a token
                limiter['tokens'] -= 1
                limiter['calls_total'] += 1
                limiter['last_call'] = now
                logger.debug(f"Rate limiter {source} consumed token, {limiter['tokens']:.2f} remaining")
    
    # Wait if needed (outside the lock to prevent deadlocks)
    if wait_time > 0:
        sleep(wait_time)
    
    return wait_time

def handle_rate_limit_error(source: str, error: Exception) -> float:
    """
    Handle rate limit error with exponential backoff.
    
    Args:
        source: Source identifier
        error: The rate limit error
        
    Returns:
        Backoff time in seconds
    """
    limiter = get_rate_limiter(source)
    
    with _rate_limiter_lock:
        # Exponential backoff
        limiter['consecutive_errors'] += 1
        limiter['errors_total'] += 1
        
        # Calculate backoff time with jitter to avoid thundering herd.
        # Cap is per-source so per-hour-quota APIs (Instagram 200/hr) can wait
        # longer than the default 5 minutes for quota replenishment.
        max_cap = limiter.get('max_backoff_seconds', 300)
        base_backoff = min(max_cap, 2 ** limiter['consecutive_errors'])
        jitter = base_backoff * 0.1  # 10% jitter
        backoff_seconds = base_backoff + random.uniform(-jitter, jitter)
        
        limiter['backoff_until'] = time() + backoff_seconds
        
        logger.warning(f"Rate limit hit for {source}, backing off for {backoff_seconds:.1f}s (error #{limiter['consecutive_errors']})")
        
    return backoff_seconds

def reset_rate_limit(source: str) -> None:
    """
    Reset rate limit after successful call.

    Args:
        source: Source identifier
    """
    with _rate_limiter_lock:
        if source not in _rate_limiters:
            return

        limiter = _rate_limiters[source]

        if limiter['consecutive_errors'] > 0:
            limiter['consecutive_errors'] = 0
            limiter['backoff_until'] = 0
            logger.info(f"Rate limit reset for {source}")

def get_rate_limit_stats(source: Optional[str] = None) -> Dict[str, Any]:
    """
    Get rate limit statistics.
    
    Args:
        source: Optional source identifier. If None, returns stats for all sources.
        
    Returns:
        Statistics dictionary
    """
    with _rate_limiter_lock:
        if source:
            if source not in _rate_limiters:
                return {}
            limiter = _rate_limiters[source]
            return {
                'calls': limiter['calls_total'],
                'waits': limiter['waits_total'],
                'wait_time': limiter['wait_time_total'],
                'errors': limiter['errors_total'],
                'current_tokens': limiter['tokens'],
                'in_backoff': time() < limiter['backoff_until'],
                'consecutive_errors': limiter['consecutive_errors']
            }
        else:
            return {
                source_name: {
                    'calls': limiter['calls_total'],
                    'waits': limiter['waits_total'],
                    'wait_time': limiter['wait_time_total'],
                    'errors': limiter['errors_total'],
                    'current_tokens': limiter['tokens'],
                    'in_backoff': time() < limiter['backoff_until']
                }
                for source_name, limiter in _rate_limiters.items()
            }

def with_rate_limit(source: str, calls_per_second: float = 1.0):
    """
    Decorator for rate-limited functions.
    
    Args:
        source: Source identifier
        calls_per_second: Maximum calls per second
        
    Returns:
        Decorator function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Wait for rate limit
            wait_if_needed(source)
            
            try:
                # Call function
                result = func(*args, **kwargs)
                
                # Reset rate limit on success
                reset_rate_limit(source)
                
                return result
            except Exception as e:
                # Check if it's a rate limit error
                error_message = str(e).lower()
                if any(term in error_message for term in ['rate limit', 'too many requests', '429']):
                    handle_rate_limit_error(source, e)
                raise
        return wrapper
    return decorator

def create_rate_limiter(calls_per_second: float = 1.0, name: str = None,
                        max_backoff_seconds: float = 300.0) -> str:
    """
    Create a new rate limiter instance.

    Args:
        calls_per_second: Maximum calls per second
        name: Optional name for the limiter
        max_backoff_seconds: Cap on exponential backoff after 429 errors.
            Default 5 min; raise for per-hour-quota APIs.

    Returns:
        Limiter name
    """
    # Generate a unique name if not provided
    if not name:
        name = f"limiter_{uuid.uuid4().hex[:8]}"

    # Initialize the limiter
    get_rate_limiter(name, calls_per_second, max_backoff_seconds=max_backoff_seconds)

    logger.info(f"Created rate limiter '{name}' with {calls_per_second} calls/second, max backoff {max_backoff_seconds}s")
    return name

def acquire_token(limiter_name: str) -> None:
    """
    Acquire a token from the rate limiter.
    
    Args:
        limiter_name: Name of the rate limiter
    """
    wait_if_needed(limiter_name)

def clear_rate_limiter(source: str) -> bool:
    """
    Clear a rate limiter (useful for testing).
    
    Args:
        source: Source identifier
        
    Returns:
        True if limiter was cleared, False if it didn't exist
    """
    with _rate_limiter_lock:
        if source in _rate_limiters:
            del _rate_limiters[source]
            logger.info(f"Cleared rate limiter for {source}")
            return True
        return False

def clear_all_rate_limiters() -> int:
    """
    Clear all rate limiters (useful for testing).
    
    Returns:
        Number of limiters cleared
    """
    with _rate_limiter_lock:
        count = len(_rate_limiters)
        _rate_limiters.clear()
        logger.info(f"Cleared {count} rate limiters")
        return count