#!/usr/bin/env python3
"""
Facebook Backfill Gap Detection

Queries BigQuery to find pages with missing dates in facebook_post_insights
and facebook_page_insights. Reports any gaps so you can re-run targeted
backfills for just the pages/dates that are missing data.

Usage:
    python facebook_gap_check.py [--start 2022-10-01] [--end 2026-05-13]
    python facebook_gap_check.py --start 2025-01-01 --end 2025-03-31

If no dates are given, checks the full known production range.
"""

import argparse
import os
import sys
from datetime import date

from google.cloud import bigquery


PROJECT = "bi-data-391216"

TABLES_TO_CHECK = [
    "facebook_data.facebook_page_insights",
    "facebook_data.facebook_post_insights",
]


def get_client():
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds or not os.path.exists(creds):
        for candidate in (
            "C:/gcp/service-account-bionews-pipeline.json",
            "/home/orchestrator/service-account-bionews-pipeline.json",
        ):
            if os.path.exists(candidate):
                creds = candidate
                break
    if creds:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds
    return bigquery.Client(project=PROJECT)


def find_missing_pages_for_range(client, table_path, start_date, end_date):
    """Return list of (page_id, page_name, missing_date_count) for the range."""
    query = f"""
    WITH known_pages AS (
        SELECT DISTINCT page_id, page_name
        FROM `{PROJECT}.{table_path}`
    ),
    date_spine AS (
        SELECT day
        FROM UNNEST(GENERATE_DATE_ARRAY('{start_date}', '{end_date}')) AS day
    ),
    expected AS (
        SELECT p.page_id, p.page_name, d.day
        FROM known_pages p
        CROSS JOIN date_spine d
    ),
    actual AS (
        SELECT DISTINCT page_id, DATE(date) AS day
        FROM `{PROJECT}.{table_path}`
        WHERE DATE(date) BETWEEN '{start_date}' AND '{end_date}'
    ),
    gaps AS (
        SELECT e.page_id, e.page_name, e.day
        FROM expected e
        LEFT JOIN actual a
          ON e.page_id = a.page_id AND e.day = a.day
        WHERE a.page_id IS NULL
    )
    SELECT page_id, page_name, COUNT(*) AS missing_days,
           MIN(day) AS earliest_missing, MAX(day) AS latest_missing
    FROM gaps
    GROUP BY page_id, page_name
    ORDER BY missing_days DESC, page_name
    """
    result = client.query(query).result()
    rows = []
    for row in result:
        rows.append(
            {
                "page_id": row.page_id,
                "page_name": row.page_name,
                "missing_days": row.missing_days,
                "earliest_missing": row.earliest_missing,
                "latest_missing": row.latest_missing,
            }
        )
    return rows


def find_actual_data_range(client, table_path):
    """Return (min_date, max_date) of actual data in the table."""
    query = f"""
    SELECT MIN(DATE(date)) AS min_d, MAX(DATE(date)) AS max_d
    FROM `{PROJECT}.{table_path}`
    """
    result = client.query(query).result()
    for row in result:
        return row.min_d, row.max_d
    return None, None


def print_banner(text):
    bar = "=" * 78
    print()
    print(bar)
    print(f"  {text}")
    print(bar)


def print_table_report(table_path, gaps):
    if not gaps:
        print(f"\n{table_path}: No gaps detected.")
        return

    print(f"\n{table_path}: {len(gaps)} page(s) have missing dates")
    print("-" * 78)
    print(
        f"  {'PAGE NAME':<35} {'PAGE ID':<20} {'MISSING':>8} {'RANGE':<25}"
    )
    print("-" * 78)
    for gap in gaps:
        name = gap["page_name"] or "(unknown)"
        if len(name) > 33:
            name = name[:32] + "..."
        date_range = f"{gap['earliest_missing']} to {gap['latest_missing']}"
        print(
            f"  {name:<35} {gap['page_id']:<20} {gap['missing_days']:>8} "
            f"{date_range:<25}"
        )


def print_remediation_commands(all_gaps):
    """Print suggested re-run commands grouped by page."""
    # Collect unique page IDs and their earliest/latest missing dates across both tables
    page_ranges = {}
    for table_path, gaps in all_gaps.items():
        for gap in gaps:
            pid = gap["page_id"]
            if pid not in page_ranges:
                page_ranges[pid] = {
                    "name": gap["page_name"],
                    "earliest": gap["earliest_missing"],
                    "latest": gap["latest_missing"],
                }
            else:
                page_ranges[pid]["earliest"] = min(
                    page_ranges[pid]["earliest"], gap["earliest_missing"]
                )
                page_ranges[pid]["latest"] = max(
                    page_ranges[pid]["latest"], gap["latest_missing"]
                )

    if not page_ranges:
        return

    print_banner("SUGGESTED REMEDIATION COMMANDS")
    print()
    print("Re-run targeted backfills for pages with gaps:")
    print()

    # Group pages with the same date range
    by_range = {}
    for pid, info in page_ranges.items():
        key = (info["earliest"], info["latest"])
        by_range.setdefault(key, []).append(pid)

    for (earliest, latest), page_ids in sorted(by_range.items()):
        ids_str = " ".join(page_ids)
        print(
            f"python orchestrate.py --source facebook --env prod "
            f"--tables pages posts page_insights post_insights "
            f"--start-date {earliest} --end-date {latest} "
            f"--sites {ids_str}"
        )
        print()


def main():
    parser = argparse.ArgumentParser(description="Detect gaps in Facebook backfill data")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    client = get_client()

    print_banner("FACEBOOK BACKFILL GAP CHECK")

    # Determine date range
    if args.start and args.end:
        start_date = args.start
        end_date = args.end
        print(f"Checking range: {start_date} to {end_date}")
    else:
        # Use union of actual data ranges across both tables
        ranges = []
        for table_path in TABLES_TO_CHECK:
            min_d, max_d = find_actual_data_range(client, table_path)
            if min_d and max_d:
                ranges.append((min_d, max_d))
                print(f"{table_path}: {min_d} to {max_d}")

        if not ranges:
            print("ERROR: No data found in any table.")
            sys.exit(1)

        start_date = str(min(r[0] for r in ranges))
        end_date = str(max(r[1] for r in ranges))
        print(f"\nUsing full known range: {start_date} to {end_date}")

    all_gaps = {}
    total_missing = 0
    for table_path in TABLES_TO_CHECK:
        gaps = find_missing_pages_for_range(client, table_path, start_date, end_date)
        all_gaps[table_path] = gaps
        print_table_report(table_path, gaps)
        total_missing += sum(g["missing_days"] for g in gaps)

    print_banner("SUMMARY")
    print(f"Date range checked: {start_date} to {end_date}")
    for table_path, gaps in all_gaps.items():
        total_for_table = sum(g["missing_days"] for g in gaps)
        print(
            f"{table_path}: {len(gaps)} page(s) with gaps, "
            f"{total_for_table:,} missing page-days"
        )

    if total_missing > 0:
        print_remediation_commands(all_gaps)
        sys.exit(1)

    print()
    print("No gaps detected.")
    sys.exit(0)


if __name__ == "__main__":
    main()
