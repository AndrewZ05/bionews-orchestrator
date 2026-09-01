#!/usr/bin/env python3
"""
Adaptive Polling Utilities
===========================

Provides intelligent polling strategies that adapt based on response patterns
to optimize for both responsiveness and resource efficiency.

Features:
- Conservative adaptive intervals (starts slow, speeds up carefully)
- Exponential backoff with jitter
- Maximum wait time enforcement
- Statistics tracking

Author: Data Pipeline Team
Created: 2025-11-25
"""

import time
import random
import logging
from typing import Callable, Optional, Any, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class PollingStats:
    """Statistics for a polling operation"""
    total_polls: int = 0
    total_wait_time: float = 0.0
    intervals_used: List[float] = None

    def __post_init__(self):
        if self.intervals_used is None:
            self.intervals_used = []


def adaptive_poll(
    check_fn: Callable[[], bool],
    success_message: str = "Condition met",
    failure_message: str = "Timeout waiting for condition",
    initial_interval: float = 5.0,
    max_interval: float = 30.0,
    max_wait_seconds: float = 3600.0,
    backoff_multiplier: float = 1.5,
    jitter: bool = True,
    log_polls: bool = True
) -> tuple[bool, PollingStats]:
    """
    Poll a condition with adaptive intervals.

    Starts with initial_interval and gradually increases up to max_interval.
    More conservative than aggressive - prioritizes not overwhelming APIs.

    Args:
        check_fn: Function that returns True when condition is met
        success_message: Message to log on success
        failure_message: Message to log on timeout
        initial_interval: Starting poll interval in seconds (default: 5)
        max_interval: Maximum poll interval in seconds (default: 30)
        max_wait_seconds: Total timeout in seconds (default: 3600 = 1 hour)
        backoff_multiplier: Multiplier for interval growth (default: 1.5)
        jitter: Add random jitter to prevent thundering herd (default: True)
        log_polls: Log each poll attempt (default: True)

    Returns:
        Tuple of (success: bool, stats: PollingStats)

    Example:
        >>> def check_job_complete():
        ...     job = api.get_job(job_id)
        ...     return job.status == 'complete'
        >>>
        >>> success, stats = adaptive_poll(
        ...     check_job_complete,
        ...     "Job completed",
        ...     initial_interval=5.0,
        ...     max_interval=30.0
        ... )
        >>> if success:
        ...     print(f"Job finished after {stats.total_wait_time}s")
    """
    stats = PollingStats()
    start_time = time.time()
    current_interval = initial_interval
    poll_count = 0

    while True:
        poll_count += 1
        stats.total_polls = poll_count

        # Check condition
        if check_fn():
            elapsed = time.time() - start_time
            stats.total_wait_time = elapsed
            if log_polls:
                logger.info(f"[OK] {success_message} (after {poll_count} polls, {elapsed:.1f}s)")
            return True, stats

        # Check timeout
        elapsed = time.time() - start_time
        if elapsed >= max_wait_seconds:
            stats.total_wait_time = elapsed
            if log_polls:
                logger.warning(f"[FAIL] {failure_message} (after {poll_count} polls, {elapsed:.1f}s)")
            return False, stats

        # Calculate next interval with optional jitter
        wait_interval = min(current_interval, max_interval)
        if jitter:
            # Add ±20% jitter to prevent synchronized polling
            jitter_factor = random.uniform(0.8, 1.2)
            wait_interval *= jitter_factor

        stats.intervals_used.append(wait_interval)

        if log_polls:
            logger.debug(f"Poll {poll_count}: Condition not met, waiting {wait_interval:.1f}s...")

        # Wait
        time.sleep(wait_interval)

        # Increase interval for next iteration
        current_interval = min(current_interval * backoff_multiplier, max_interval)


def poll_async_job(
    get_status_fn: Callable[[], str],
    completed_statuses: List[str],
    failed_statuses: List[str],
    job_description: str = "Job",
    initial_interval: float = 5.0,
    max_interval: float = 30.0,
    max_wait_seconds: float = 3600.0
) -> tuple[bool, str, PollingStats]:
    """
    Poll an async job until completion or failure.

    Conservative adaptive polling specifically designed for async jobs
    (Facebook Insights, DCM Reports, Mailchimp Batches, etc.)

    Args:
        get_status_fn: Function that returns current job status string
        completed_statuses: List of status values that mean success (e.g., ['complete', 'finished'])
        failed_statuses: List of status values that mean failure (e.g., ['failed', 'error'])
        job_description: Description for logging (e.g., "Facebook Insights Job")
        initial_interval: Starting interval in seconds (default: 5)
        max_interval: Maximum interval in seconds (default: 30)
        max_wait_seconds: Total timeout in seconds (default: 3600 = 1 hour)

    Returns:
        Tuple of (success: bool, final_status: str, stats: PollingStats)

    Example:
        >>> def get_job_status():
        ...     job = facebook_job.api_get()
        ...     return job.async_status
        >>>
        >>> success, status, stats = poll_async_job(
        ...     get_job_status,
        ...     completed_statuses=['Job Completed'],
        ...     failed_statuses=['Job Failed'],
        ...     job_description="Facebook Campaign Insights"
        ... )
    """
    final_status = None

    def check_job():
        nonlocal final_status
        final_status = get_status_fn()

        if final_status in completed_statuses:
            return True
        elif final_status in failed_statuses:
            # Job failed, stop polling
            raise Exception(f"{job_description} failed with status: {final_status}")
        else:
            # Still running
            return False

    try:
        success, stats = adaptive_poll(
            check_fn=check_job,
            success_message=f"{job_description} completed",
            failure_message=f"{job_description} timeout",
            initial_interval=initial_interval,
            max_interval=max_interval,
            max_wait_seconds=max_wait_seconds,
            log_polls=True
        )
        return success, final_status, stats

    except Exception as e:
        # Job failed (not timeout)
        logger.error(f"{job_description} error: {e}")
        elapsed = time.time()
        stats = PollingStats(total_polls=1, total_wait_time=0.0)
        return False, final_status, stats


def conservative_sleep(
    base_seconds: float,
    iteration: int = 0,
    max_seconds: float = 60.0,
    add_jitter: bool = True
) -> float:
    """
    Conservative sleep with optional backoff and jitter.

    Use this for simple delays that should adapt over time.

    Args:
        base_seconds: Base sleep duration
        iteration: Current iteration (0-indexed), used for backoff
        max_seconds: Maximum sleep duration
        add_jitter: Add random jitter (default: True)

    Returns:
        Actual sleep duration used

    Example:
        >>> for i in range(10):
        ...     result = check_status()
        ...     if result.ready:
        ...         break
        ...     conservative_sleep(5.0, iteration=i, max_seconds=30.0)
    """
    # Calculate sleep with exponential backoff
    sleep_duration = min(base_seconds * (1.5 ** iteration), max_seconds)

    # Add jitter
    if add_jitter:
        jitter_factor = random.uniform(0.8, 1.2)
        sleep_duration *= jitter_factor

    logger.debug(f"Sleeping for {sleep_duration:.1f}s (iteration {iteration})")
    time.sleep(sleep_duration)
    return sleep_duration
