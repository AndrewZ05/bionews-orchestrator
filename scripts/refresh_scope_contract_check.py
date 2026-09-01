#!/usr/bin/env python3
"""
Static check: ensure refresh-scope change sources are declared and consistent,
and that sql/profile_database_refresh.sql STEP 0 (_changed_bn_ids) keeps the
same eligibility predicates (hub latest row + bot gate, GA4 session_start +
xref bot gate) as profile_database_refresh_scope.sql.

STEP 0 intentionally does NOT require profile_core to exist (net-new identities
and identifier-linked activity must enter refresh like other ETL upserts).

Contracts:

1) sql/profile_database_refresh_scope.sql declares scope sources in a parseable
   header block and literals match the header.
2) sql/profile_database_refresh.sql STEP 0 contains required marker substrings so
   scope table and _changed_bn_ids cannot silently drift, and must not reintroduce
   a profile_core EXISTS gate on change detection.

Usage:
    python scripts/refresh_scope_contract_check.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
REFRESH_SCOPE_SQL = REPO_ROOT / "sql" / "profile_database_refresh_scope.sql"
REFRESH_SQL = REPO_ROOT / "sql" / "profile_database_refresh.sql"

# Substrings that must appear in STEP 0 of profile_database_refresh.sql (must stay
# aligned with profile_database_refresh_scope.sql population logic).
_CHANGED_BN_IDS_MARKERS: tuple[str, ...] = (
    "QUALIFY ROW_NUMBER() OVER (PARTITION BY bn_id ORDER BY last_seen DESC) = 1",
    "identity_hub_data.bn_id_xref",
    "FROM mailchimp_data.members m",
    "FROM mailchimp_data.campaign_email_activity ea",
    "FROM wordpress_data.wordpress_users u",
    "FROM df_warehouse_output.output_ga4_sessions s",
    "s.session_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)",
    "AND IFNULL(x.is_bot, FALSE) = FALSE",
    "AND IFNULL(x.is_shared_workstation, FALSE) = FALSE",
)

_SCOPE_MARKERS: tuple[str, ...] = (
    "QUALIFY ROW_NUMBER() OVER (PARTITION BY bn_id ORDER BY last_seen DESC) = 1",
    "identity_hub_data.bn_id_xref",
    "FROM mailchimp_data.members m",
    "FROM mailchimp_data.campaign_email_activity ea",
    "FROM wordpress_data.wordpress_users u",
    "FROM df_warehouse_output.output_ga4_sessions s",
    "s.session_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)",
    "AND IFNULL(x.is_bot, FALSE) = FALSE",
    "AND IFNULL(x.is_shared_workstation, FALSE) = FALSE",
)

_PROFILE_CORE_EXISTS_SNIPPET = (
    "EXISTS (SELECT 1 FROM profile_data.profile_core c WHERE c.bn_id ="
)

BEGIN = "-- REFRESH_SCOPE_SOURCES_BEGIN"
END = "-- REFRESH_SCOPE_SOURCES_END"


def parse_declared_sources(text: str) -> list[str]:
    if BEGIN not in text or END not in text:
        raise ValueError(
            f"Missing refresh-scope source contract markers ({BEGIN} / {END})."
        )
    block = text.split(BEGIN, 1)[1].split(END, 1)[0]
    sources: list[str] = []
    for line in block.splitlines():
        line = line.strip()
        if not line.startswith("--"):
            continue
        item = line[2:].strip()
        if not item.startswith("-"):
            continue
        value = item[1:].strip()
        if value:
            sources.append(value)
    if not sources:
        raise ValueError("No declared refresh-scope sources found in header block.")
    return sources


def extract_changed_bn_ids_block(text: str) -> str:
    needle = "CREATE OR REPLACE TEMP TABLE _changed_bn_ids AS"
    start = text.find(needle)
    if start == -1:
        raise ValueError("profile_database_refresh.sql: missing _changed_bn_ids temp table")
    step1 = text.find("-- STEP 1:", start)
    if step1 == -1:
        raise ValueError("profile_database_refresh.sql: missing STEP 1 delimiter after _changed_bn_ids")
    return text[start:step1]


def check_refresh_sql_step0_aligned() -> None:
    body = REFRESH_SQL.read_text(encoding="utf-8")
    block = extract_changed_bn_ids_block(body)
    for marker in _CHANGED_BN_IDS_MARKERS:
        if marker not in block:
            raise ValueError(
                f"profile_database_refresh.sql STEP 0 missing marker (refresh/scope drift?): {marker!r}"
            )
    if _PROFILE_CORE_EXISTS_SNIPPET in block:
        raise ValueError(
            "profile_database_refresh.sql STEP 0: must not gate _changed_bn_ids on "
            "EXISTS(profile_core); net-new identities must be eligible for refresh."
        )


def check_refresh_scope_sql_markers() -> None:
    text = REFRESH_SCOPE_SQL.read_text(encoding="utf-8")
    for marker in _SCOPE_MARKERS:
        if marker not in text:
            raise ValueError(
                f"profile_database_refresh_scope.sql missing marker: {marker!r}"
            )
    if _PROFILE_CORE_EXISTS_SNIPPET in text:
        raise ValueError(
            "profile_database_refresh_scope.sql: must not gate refresh scope on "
            "EXISTS(profile_core); net-new identities must be in scope."
        )


def parse_actual_sources(text: str) -> list[str]:
    # Match the literal label used in:
    #   SELECT DISTINCT <expr>, 'label', CURRENT_TIMESTAMP()
    # The refresh-scope SQL uses this pattern for all inserts.
    labels = re.findall(r"SELECT\s+DISTINCT\s+[^;]*?,\s*'([^']+)'\s*,\s*CURRENT_TIMESTAMP\(\)", text, re.IGNORECASE)
    return sorted(set(labels))


def main() -> int:
    try:
        text = REFRESH_SCOPE_SQL.read_text(encoding="utf-8")
        declared = parse_declared_sources(text)
        actual = parse_actual_sources(text)

        declared_set = set(declared)
        actual_set = set(actual)

        missing = sorted(actual_set - declared_set)
        extra = sorted(declared_set - actual_set)

        if missing or extra:
            print("[FAIL] Refresh-scope source contract mismatch.")
            if missing:
                print(f"  Missing in header: {missing}")
            if extra:
                print(f"  Declared but not inserted: {extra}")
            return 1

        print(f"[OK ] Refresh-scope sources declared and consistent: {sorted(actual_set)}")

        check_refresh_scope_sql_markers()
        print("[OK ] Refresh-scope SQL marker predicates present (hub/GA4/bot; no profile_core gate).")

        check_refresh_sql_step0_aligned()
        print("[OK ] profile_database_refresh.sql STEP 0 markers match scope contract (no drift).")

        return 0
    except ValueError as exc:
        print(f"[FAIL] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
