#!/usr/bin/env python3
"""Quick operational snapshot of the profile database.

This is an ad hoc utility script kept in ``shared/`` because it is still part
of the working profile project toolbox, but it is not part of the runtime
pipeline. It writes a small human-readable report to the repo root.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from google.cloud import bigquery

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_PATH = REPO_ROOT / "profile_db_results.txt"
DEFAULT_PROJECT_ID = "bi-data-391216"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write a quick profile DB health report.")
    parser.add_argument("--project", default=DEFAULT_PROJECT_ID, help="BigQuery project id")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="Output file path")
    parser.add_argument("--credentials", default=None, help="Optional GOOGLE_APPLICATION_CREDENTIALS path override")
    return parser.parse_args()


args = parse_args()
OUTPUT_PATH = Path(args.output)

if args.credentials:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.credentials

client = bigquery.Client(project=args.project)


def q(handle, label: str, sql: str) -> None:
    handle.write(f"\n=== {label} ===\n")
    handle.flush()
    for row in client.query(sql).result():
        values = dict(row)
        for key, value in values.items():
            if isinstance(value, (int, float)):
                handle.write(f"  {key:45} {value:>15,}\n")
            else:
                handle.write(f"  {key:45} {value}\n")
    handle.flush()


with OUTPUT_PATH.open("w", encoding="utf-8") as output:
    q(output, "Row counts", "SELECT 'profile_core' AS tbl, COUNT(*) AS n FROM profile_data.profile_core UNION ALL SELECT 'profile_engagement', COUNT(*) FROM profile_data.profile_engagement UNION ALL SELECT 'profile_identifiers', COUNT(*) FROM profile_data.profile_identifiers UNION ALL SELECT 'profile_current', COUNT(*) FROM profile_data.profile_current UNION ALL SELECT 'profile_exceptions', COUNT(*) FROM profile_data.profile_exceptions ORDER BY n DESC")
    q(output, "Role flag counts (v6.5; each user can have multiple flags TRUE)", "SELECT role, COUNT(*) AS n, ROUND(100.0*COUNT(*)/(SELECT COUNT(*) FROM profile_data.profile_core),1) AS pct FROM profile_data.profile_roles, UNNEST(roles) AS role GROUP BY role ORDER BY n DESC")
    q(output, "Engagement tiers", "SELECT COALESCE(engagement_tier,'(null)') AS tier, COUNT(*) AS n FROM profile_data.profile_engagement GROUP BY engagement_tier ORDER BY n DESC")
    q(output, "Fill rates", "SELECT COUNT(*) AS total, ROUND(100.0*COUNTIF(email IS NOT NULL)/COUNT(*),1) AS email_pct, ROUND(100.0*COUNTIF(first_name IS NOT NULL)/COUNT(*),1) AS name_pct, ROUND(100.0*COUNTIF(preferred_condition IS NOT NULL)/COUNT(*),1) AS condition_pct, ROUND(100.0*COUNTIF(hcp_status=TRUE)/COUNT(*),1) AS hcp_pct FROM profile_data.profile_core")
    q(output, "Audiences", "SELECT COUNT(*) AS total, COUNTIF(has_any_interaction=TRUE) AS active, COUNTIF(is_marketing_eligible=TRUE) AS mktg_eligible, COUNTIF(is_hcp_targetable=TRUE) AS hcp_target, COUNTIF(is_personalization_eligible=TRUE) AS personalize FROM profile_data.profile_current")
    q(output, "Top conditions", "SELECT preferred_condition.label AS cond, COUNT(*) AS n FROM profile_data.profile_core WHERE preferred_condition IS NOT NULL GROUP BY cond ORDER BY n DESC LIMIT 10")
    q(output, "Duplicates", "SELECT COUNT(*) AS dup_bn_ids FROM (SELECT bn_id FROM profile_data.profile_current GROUP BY bn_id HAVING COUNT(*)>1)")
    q(output, "Build log", "SELECT step_name, status, rows_affected, ROUND(duration_seconds,1) AS secs FROM profile_ops.profile_build_steps WHERE started_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 HOUR) ORDER BY started_at ASC")
    output.write("\nDONE\n")

print(f"Results written to {OUTPUT_PATH}")
