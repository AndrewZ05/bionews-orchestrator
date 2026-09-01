#!/usr/bin/env python3
# Table Freshness Auditing subpackage.
# Houses the leaf modules (status rules, config loader, validator, calculator,
# result writer) and the orchestration runner used by the table_freshness plugin.
#
# This module also exposes the single shared identifier-validation helper so that
# every SQL builder (config_loader, table_validator, freshness_calculator,
# result_writer) uses identical allowlist logic and the validation does not drift.
# BigQuery cannot bind dataset/table/column names as @parameters, so any token
# interpolated into SQL MUST first pass through _validate_identifier.
"""Table Freshness Auditing package.

Provides the building blocks for probing configured BigQuery tables for staleness
and recording PASS/WARNING/FAIL/ERROR results. See the individual submodules for
details. All log strings and SQL in this package are ASCII-only and use double-dash
SQL comments exclusively.
"""

import re

# Allowlist for SQL identifiers (dataset / table / column names). BigQuery
# dataset/table/column names here are restricted to ASCII letters, digits and
# underscore, optionally beginning with an underscore. Anything else (dots,
# backticks, spaces, quotes, dashes) is rejected to prevent SQL injection through
# interpolated names.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Allowlist for GCP PROJECT IDs, which differ from BQ identifiers: they are
# lowercase letters, digits and HYPHENS, 6-30 chars, starting with a letter
# (e.g. "bi-data-391216"). Hyphens are legal here but still no dots/quotes/spaces,
# so interpolation stays injection-safe. A trailing "(:region)" style suffix is
# not supported -- project must be the bare id.
_PROJECT_ID_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")


def _validate_identifier(name: str, kind: str) -> str:
    """Validate a single BigQuery identifier (project, dataset, table, or column).

    BigQuery does not allow project/dataset/table/column names to be bound as
    query parameters, so callers must interpolate them directly into the SQL. To
    make that safe, every such token is validated here against a strict allowlist
    BEFORE it is interpolated.

    Project IDs use a hyphen-permitting allowlist (kind == 'project'); all other
    identifiers use the underscore-only BQ name allowlist.

    Args:
        name: the candidate identifier.
        kind: a short label (e.g. 'project', 'dataset', 'table', 'date_column')
            used to pick the allowlist and in the ASCII error message.

    Returns:
        The validated name unchanged (so callers can write
        ``ds = _validate_identifier(ds, 'dataset')``).

    Raises:
        ValueError: if name is not a non-empty string matching the allowlist.
    """
    if not isinstance(name, str) or not name:
        # Reject None / empty / non-string before the regex to give a clear message.
        raise ValueError("invalid {0} identifier: empty or non-string".format(kind))
    # Project IDs legitimately contain hyphens; everything else is BQ-name strict.
    pattern = _PROJECT_ID_RE if kind == "project" else _IDENTIFIER_RE
    if not pattern.match(name):
        # ASCII-only message; never echo unicode. Keep the offending value for ops.
        raise ValueError("invalid {0} identifier: {1}".format(kind, name))
    return name
