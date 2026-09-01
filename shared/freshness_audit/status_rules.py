#!/usr/bin/env python3
# Pure status-determination rules for the Table Freshness audit.
# No BigQuery, no IO -- importable in isolation by unit tests.
"""Freshness audit status rules.

Encodes the STATUS RULES exactly and exposes the status string constants so that
no other module hardcodes the literals. The ERROR status is owned by the
calculator/runner (empty table, validation failure, query error) and is NEVER
returned by determine_status, which assumes a valid integer days_behind.
"""

import logging

logger = logging.getLogger(__name__)

# Status string constants. Other modules import these instead of hardcoding
# literals so the spelling/case can never drift between producer and consumer.
STATUS_PASS = "PASS"
STATUS_WARNING = "WARNING"
STATUS_FAIL = "FAIL"
STATUS_ERROR = "ERROR"
# STATIC: an intentionally-unchanging reference/config table (cadence: static).
# Distinct from PASS, which implies "freshly loaded and current". STATIC says
# "freshness is not the right question -- this data is complete and does not
# change". It is still backed by a real (large) freshness threshold so a TRULY
# broken static table (gone past the static threshold, or 0 rows / probe error
# routed to ERROR by the caller) still surfaces -- static does not mean unchecked.
STATUS_STATIC = "STATIC"


def determine_status(
    days_behind,
    warning_threshold_days,
    freshness_threshold_days,
    is_static=False,
):
    """Determine the freshness status for a table from its computed days_behind.

    Status rules (ordering is intentional and tested):
      * FAIL    if days_behind > freshness_threshold_days
      * STATIC  if is_static and days_behind <= freshness_threshold_days (an
                intentionally-unchanging table that is within its large static
                threshold -- shown distinctly from PASS, but a static table that
                blows past even its static threshold still FAILs above)
      * WARNING if days_behind >= warning_threshold_days (and <= freshness, the
                earlier alarm, since warning_threshold_days <= freshness_threshold_days)
      * PASS    otherwise

    ERROR is not produced here; the caller routes the no-days_behind case (empty
    table, validation/query failure) through the ERROR path. A None days_behind is
    therefore a programming error from this function's point of view and raises
    ValueError so callers do not silently mis-classify it as PASS.

    Args:
        days_behind: integer number of days between CURRENT_DATE() and the table's
            most recent date. Must not be None.
        warning_threshold_days: the earlier alarm threshold (inclusive).
        freshness_threshold_days: the hard staleness threshold (inclusive for WARNING,
            exceeded triggers FAIL).
        is_static: True when the target's cadence is 'static' -- a within-threshold
            result is reported as STATIC rather than PASS/WARNING.

    Returns:
        One of STATUS_PASS, STATUS_WARNING, STATUS_FAIL, STATUS_STATIC.

    Raises:
        ValueError: if days_behind is None.
    """
    if days_behind is None:
        # Route the no-value case through ERROR at the caller, not silently to PASS.
        raise ValueError("days_behind is None -- caller must use the ERROR path")

    # Check FAIL first: strictly beyond the hard threshold means stale. This still
    # applies to static tables -- exceeding even the large static threshold is a
    # genuine "this should be unchanging but went missing/way-stale" signal.
    if days_behind > freshness_threshold_days:
        return STATUS_FAIL
    # A static table within its threshold is STATIC (not PASS/WARNING): freshness
    # is not the relevant question for intentionally-unchanging reference data.
    if is_static:
        return STATUS_STATIC
    # WARNING is the earlier alarm: at or past the warning threshold but still within
    # the hard freshness threshold (guaranteed by warning <= freshness).
    if days_behind >= warning_threshold_days:
        return STATUS_WARNING
    # Otherwise the table is fresh enough.
    return STATUS_PASS
