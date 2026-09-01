#!/usr/bin/env python3
"""
Initial fixture-driven SQL module tests for the profile database.

This is not a full end-to-end rebuild harness. It exercises the highest-value
business-logic fragments in isolated BigQuery queries so regressions are caught
before a multi-million-row rebuild is required.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

from google.cloud import bigquery


REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from shared.bigquery_client import close_bigquery_client, get_bigquery_client  # noqa: E402


PROJECT_ID = "bi-data-391216"
CASES_PATH = REPO_ROOT / "tests" / "profile_database" / "profile_sql_module_cases.json"


def sql_string(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def sql_int(value: int | None) -> str:
    return "NULL" if value is None else str(value)


def load_cases() -> dict:
    return json.loads(CASES_PATH.read_text(encoding="utf-8"))


def run_query(client: bigquery.Client, sql: str) -> list[dict]:
    rows = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(use_query_cache=False),
    ).result()
    return [dict(row) for row in rows]


def run_mailchimp_condition_mapping_suite(client: bigquery.Client, fixtures: dict) -> None:
    lookup_rows = fixtures["lookup_rows"]
    cases = fixtures["cases"]

    lookup_sql = "\nUNION ALL\n".join(
        (
            f"SELECT {sql_string(row['list_abbrev'])} AS list_abbrev, "
            f"{sql_string(row['condition_key'])} AS condition_key"
        )
        for row in lookup_rows
    )
    case_sql = "\nUNION ALL\n".join(
        (
            f"SELECT {idx} AS case_id, {sql_string(case['list_name'])} AS list_name, "
            f"{sql_string(case['expected_condition_key'])} AS expected_condition_key, "
            f"{sql_string(case['expected_site_key'])} AS expected_site_key"
        )
        for idx, case in enumerate(cases, start=1)
    )

    sql = f"""
    WITH lookup_rows AS (
        {lookup_sql}
    ),
    cases AS (
        {case_sql}
    ),
    resolved AS (
        SELECT
            c.case_id,
            c.list_name,
            lr.condition_key,
            CASE
                WHEN lr.list_abbrev IS NULL THEN NULL
                ELSE LOWER(lr.list_abbrev)
            END AS site_key,
            c.expected_condition_key,
            c.expected_site_key
        FROM cases c
        LEFT JOIN lookup_rows lr
            ON UPPER(c.list_name) = lr.list_abbrev
    )
    SELECT *
    FROM resolved
    ORDER BY case_id
    """

    rows = run_query(client, sql)
    failures = [
        row for row in rows
        if row["condition_key"] != row["expected_condition_key"]
        or row["site_key"] != row["expected_site_key"]
    ]
    if failures:
        raise AssertionError(f"Mailchimp condition mapping suite failed: {failures[:3]}")


def run_preferred_condition_decay_suite(client: bigquery.Client, cases: list[dict]) -> None:
    case_sql = "\nUNION ALL\n".join(
        (
            f"SELECT {idx} AS case_id, {sql_string(case['source'])} AS preferred_condition_source, "
            f"{sql_int(case['age_days'])} AS age_days, {case['expected_confidence']} AS expected_confidence"
        )
        for idx, case in enumerate(cases, start=1)
    )

    sql = f"""
    WITH cases AS (
        {case_sql}
    ),
    resolved AS (
        SELECT
            case_id,
            preferred_condition_source,
            age_days,
            ROUND(
                (CASE preferred_condition_source
                    WHEN 'app_confirmed' THEN 1.0
                    WHEN 'mailchimp_list' THEN 0.9
                    WHEN 'mailchimp_tag' THEN 0.7
                    WHEN 'content_affinity' THEN 0.5
                    ELSE 0.5
                END)
                *
                (CASE
                    WHEN age_days IS NULL THEN 0.2
                    WHEN age_days < 90 THEN 1.0
                    WHEN age_days < 365 THEN 0.8
                    WHEN age_days < 730 THEN 0.6
                    WHEN age_days < 1095 THEN 0.4
                    ELSE 0.2
                END),
                3
            ) AS actual_confidence,
            expected_confidence
        FROM cases
    )
    SELECT *
    FROM resolved
    ORDER BY case_id
    """

    rows = run_query(client, sql)
    failures = [
        row for row in rows
        if not math.isclose(
            float(row["actual_confidence"] or 0.0),
            float(row["expected_confidence"] or 0.0),
            abs_tol=1e-9,
        )
    ]
    if failures:
        raise AssertionError(f"Preferred-condition decay suite failed: {failures[:3]}")


def run_persona_inference_suite(client: bigquery.Client, cases: list[dict]) -> None:
    case_sql = "\nUNION ALL\n".join(
        (
            f"SELECT {idx} AS case_id, {sql_string(case['describe_text'])} AS describe_text, "
            f"{sql_string(case['expected_account_type'])} AS expected_account_type"
        )
        for idx, case in enumerate(cases, start=1)
    )

    sql = f"""
    WITH cases AS (
        {case_sql}
    ),
    resolved AS (
        SELECT
            case_id,
            describe_text,
            CASE
                WHEN REGEXP_CONTAINS(LOWER(COALESCE(describe_text, '')), r'\\bpatient\\b|diagnosed|living with|person with')
                    THEN 'patient'
                WHEN REGEXP_CONTAINS(LOWER(COALESCE(describe_text, '')), r'\\bcaregiver\\b|caretaker|care\\s*giver|caring for')
                    THEN 'caregiver'
                WHEN REGEXP_CONTAINS(LOWER(COALESCE(describe_text, '')), r'\\bfamily\\b|family member|parent of|spouse of|loved one|relative|\\bfriend\\b')
                    THEN 'family_or_friend'
                WHEN REGEXP_CONTAINS(LOWER(COALESCE(describe_text, '')), r'\\bhcp\\b|healthcare|physician|doctor|nurse|provider|clinician|medical professional|researcher')
                    THEN 'hcp'
                ELSE NULL
            END AS actual_account_type,
            expected_account_type
        FROM cases
    )
    SELECT *
    FROM resolved
    ORDER BY case_id
    """

    rows = run_query(client, sql)
    failures = [
        row for row in rows
        if row["actual_account_type"] != row["expected_account_type"]
    ]
    if failures:
        raise AssertionError(f"Persona inference suite failed: {failures[:3]}")


def main() -> int:
    fixtures = load_cases()
    client = get_bigquery_client(project=PROJECT_ID)
    try:
        run_mailchimp_condition_mapping_suite(client, fixtures["mailchimp_condition_mapping"])
        print("[OK ] mailchimp_condition_mapping")

        run_preferred_condition_decay_suite(client, fixtures["preferred_condition_decay"])
        print("[OK ] preferred_condition_decay")

        run_persona_inference_suite(client, fixtures["persona_inference"])
        print("[OK ] persona_inference")
    finally:
        close_bigquery_client()

    print("[OK ] profile SQL module tests passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
