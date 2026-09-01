#!/usr/bin/env python3
# Cadence learning for the recency-lag detector.
# PURE functions over a list of datetime.date: no BigQuery, no I/O, no logging side
# effects on the data path. The recency detector queries the DISTINCT present dates
# once and hands them here to learn (a) the expected number of calendar days
# between consecutive present dates and (b) the set of weekdays the table ever
# produces, so weekday-only tables do not false-gap on weekends.
"""Cadence learning helpers for the anomaly audit recency detector.

These are deliberately PURE: every function takes a list/range of
``datetime.date`` and returns a deterministic answer with no I/O. They all tolerate
empty or very short input by degrading to a safe default (daily cadence, all-seven
active weekdays) so the recency detector never raises on a brand-new table.

All log strings are ASCII-only.
"""

import logging
from collections import Counter
from datetime import date, timedelta
from typing import List

logger = logging.getLogger(__name__)

# Minimum span (in calendar days, first present date to last) before we trust the
# learned cadence / weekday set. Below this we degrade to daily + all weekdays so a
# young table is judged by a strict daily expectation rather than a noisy guess.
MIN_HISTORY_DAYS = 30

# The full Mon..Sun weekday set (date.weekday(): Mon=0 .. Sun=6).
_ALL_WEEKDAYS = frozenset(range(7))


def _clean_sorted_unique(dates: List[date]) -> List[date]:
    """Return the input as a sorted list of unique, non-None datetime.date values.

    Tolerates None entries and duplicates so callers can pass raw query output.
    """
    cleaned = sorted({d for d in (dates or []) if d is not None})
    return cleaned


def learn_cadence(dates: List[date]) -> int:
    """Infer the expected number of calendar days between consecutive present dates.

    The cadence is the MODE of the gap sizes between consecutive DISTINCT present
    dates (e.g. 1 for a daily table, 7 for a weekly table). It is computed ONLY
    when the history spans at least ``MIN_HISTORY_DAYS`` (30) calendar days, so a
    young or sparse table degrades to a strict daily expectation rather than a
    misleading large cadence.

    Args:
        dates: present distinct dates (any order; None/dupes tolerated).

    Returns:
        The learned cadence in days. Returns 1 (daily) when there is insufficient
        history, fewer than two distinct dates, or no clear positive mode.
    """
    cleaned = _clean_sorted_unique(dates)
    if len(cleaned) < 2:
        # Need at least two dates to measure any gap; degrade to daily.
        return 1

    span_days = (cleaned[-1] - cleaned[0]).days
    if span_days < MIN_HISTORY_DAYS:
        # Not enough history to trust a learned cadence -- assume daily.
        return 1

    gaps = [
        (cleaned[i] - cleaned[i - 1]).days
        for i in range(1, len(cleaned))
        if (cleaned[i] - cleaned[i - 1]).days > 0
    ]
    if not gaps:
        return 1

    counts = Counter(gaps)
    # most_common ties break by first-seen insertion order in Counter; to make the
    # result fully deterministic and prefer the tighter cadence on a tie, pick the
    # smallest gap among those sharing the maximum frequency.
    top_freq = max(counts.values())
    best = min(gap for gap, freq in counts.items() if freq == top_freq)
    return best if best > 0 else 1


def typical_gap(dates: List[date]) -> float:
    """Return the table's TYPICAL spacing (median gap in days) between loads.

    Unlike learn_cadence (which takes the mode and is meaningful only for regular
    daily/weekly tables), this captures the central tendency of an IRREGULAR load
    schedule -- e.g. a reference table snapshotted every couple of weeks. Used to
    judge whether a specific gap is abnormally large relative to the table's own
    normal rhythm, instead of assuming a daily calendar.

    Returns 1.0 when there are fewer than two dates (degrade to daily).
    """
    import statistics

    cleaned = _clean_sorted_unique(dates)
    if len(cleaned) < 2:
        return 1.0
    gaps = [
        (cleaned[i] - cleaned[i - 1]).days
        for i in range(1, len(cleaned))
        if (cleaned[i] - cleaned[i - 1]).days > 0
    ]
    if not gaps:
        return 1.0
    return float(statistics.median(gaps))


def abnormal_gaps(dates: List[date], multiplier: float = 3.0, min_gap_days: int = 3):
    """Return (gap_start, gap_end, gap_days) for gaps abnormally larger than typical.

    A gap between two consecutive present dates is "abnormal" when it exceeds BOTH:
      * ``multiplier`` times the table's typical (median) gap, AND
      * ``min_gap_days`` calendar days (so a daily table's normal 1-day rhythm does
        not flag a routine 2-3 day blip, and irregular tables are judged against
        their own spacing).

    This replaces per-calendar-day gap enumeration: an irregularly-loaded snapshot
    table (e.g. LimeSurvey reference tables loaded every ~2 weeks) no longer flags
    every in-between calendar day -- only a genuinely unusual pause is reported.

    Args:
        dates: present distinct dates (any order; None/dupes tolerated).
        multiplier: how many times the typical gap counts as abnormal (default 3).
        min_gap_days: absolute floor so tiny cadences do not over-flag (default 3).

    Returns:
        A list of (start_date, end_date, gap_days) tuples, one per abnormal gap,
        in chronological order. Empty when history is too short or nothing is
        abnormal.
    """
    cleaned = _clean_sorted_unique(dates)
    if len(cleaned) < 3:
        # Need a few points to establish a "typical" spacing to deviate from.
        return []
    median_gap = typical_gap(cleaned)
    threshold = max(multiplier * median_gap, float(min_gap_days))
    out = []
    for i in range(1, len(cleaned)):
        gap = (cleaned[i] - cleaned[i - 1]).days
        if gap > threshold:
            out.append((cleaned[i - 1], cleaned[i], gap))
    return out


def active_weekdays(dates: List[date]) -> set:
    """Return the set of weekday ints (Mon=0..Sun=6) the table EVER produces.

    Used so weekday-only tables (which never emit Saturday/Sunday dates) are not
    flagged for a "gap" on weekends. If all seven weekdays appear, or if there is
    too little history to tell (span < ``MIN_HISTORY_DAYS``), the full set
    ``{0..6}`` is returned so no legitimate day is ever treated as never-produced.

    Args:
        dates: present distinct dates (any order; None/dupes tolerated).

    Returns:
        A set of weekday ints. Always a subset of {0,1,2,3,4,5,6}; never empty for
        non-empty input.
    """
    cleaned = _clean_sorted_unique(dates)
    if not cleaned:
        # No history: cannot exclude any weekday, so accept all.
        return set(_ALL_WEEKDAYS)

    span_days = (cleaned[-1] - cleaned[0]).days
    observed = {d.weekday() for d in cleaned}
    if span_days < MIN_HISTORY_DAYS:
        # Too short to conclude the table is weekday-only; accept all weekdays.
        return set(_ALL_WEEKDAYS)
    if observed >= _ALL_WEEKDAYS:
        # All seven seen at least once -> not a restricted-weekday table.
        return set(_ALL_WEEKDAYS)
    return observed


def expected_present_dates(start: date, end: date, weekdays: set) -> List[date]:
    """Yield every calendar date in [start, end] whose weekday() is in ``weekdays``.

    Used by the recency detector to enumerate the dates that SHOULD be present
    (given the learned active-weekday set) so interior gaps can be computed without
    flagging never-produced weekdays (e.g. weekends on a weekday-only table).

    Args:
        start: inclusive start date.
        end: inclusive end date.
        weekdays: the active-weekday set from :func:`active_weekdays`. An empty or
            falsy set is treated as "all weekdays" so the helper never returns an
            empty list for a valid date range.

    Returns:
        A sorted list of dates in [start, end] on active weekdays. Empty when
        start > end or either bound is None.
    """
    if start is None or end is None or start > end:
        return []
    active = set(weekdays) if weekdays else set(_ALL_WEEKDAYS)
    result = []
    current = start
    while current <= end:
        if current.weekday() in active:
            result.append(current)
        current = current + timedelta(days=1)
    return result
